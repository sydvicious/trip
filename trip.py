#!/usr/bin/env python

import argparse
import copy
import datetime
import math
import Queue
import sys
import threading
import time

def valid_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        msg = "Note a valide date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)

parser = argparse.ArgumentParser(description='Baseball Trip Generator')
parser.add_argument('--foo', nargs='?', help='foo help')
parser.add_argument('--city', dest='start_city', default='Austin')
parser.add_argument('--verbose', action='store_true')
parser.add_argument('--log-file', dest='log_file', type=argparse.FileType('w'))
parser.add_argument('--start-day', dest='start_day', type=int, default=0)
parser.add_argument('--start-date', dest='start_date', type=valid_date, default=None)
parser.add_argument('--destinations', dest='dest_file', default='destinations.txt', type=argparse.FileType('rU'))
parser.add_argument('--schedule-file', dest='schedule_file', default='schedule.txt', type=argparse.FileType('rU'))
parser.add_argument('--distance-file', dest='distances_file', default='distances.txt', type=argparse.FileType('rU'))
parser.add_argument('--traverse-style', dest='traverse_style', choices=['normal', 'sorted', 'parallel'], default='sorted')
args = parser.parse_args()

def get_distances():
    return_distances = {}

    with args.distances_file as f:
        line = f.readline().strip()
        entries = line.split('\t')
        cities_count = int(entries[0])
        city_list = entries[1:]
        for from_city_index in range(0, cities_count):
            from_city = city_list[from_city_index]
            distances = {}
            distance_entries = f.readline().strip().split('\t')
            from_city_check = distance_entries[0]
            if from_city != from_city_check:
                exit(1)
            for distance_index in xrange(0, cities_count):
                to_city = city_list[distance_index]
                if from_city != to_city:
                    entry = float(distance_entries[distance_index + 1])
                    days_float = (entry - 4) / 8.0
                    days = math.ceil(days_float)
                    distance = int(max(days, 0))
                    distances[to_city] = distance

                return_distances[from_city] = distances

    return city_list, return_distances


def get_wait_times():
    return_wait_times = {}

    with args.schedule_file as f:
        line = f.readline().strip()
        entries = line.split('\t')
        first_date_string = entries[1]

        for line in f.read().splitlines(False):
            fields = line.split('\t')
            city = fields[0]
            wait_times = fields[1:]
            return_wait_times[city] = wait_times
    return first_date_string, return_wait_times


def read_dest_list():
    with args.dest_file as f:
        destinations = f.read().strip().split('\n')
    return set(destinations)


def get_worker(traversal):
    while True:
        intermediate = traversal.results.get()
        if intermediate.time_so_far >= traversal.best_time:
            continue
        for i in range(num_worker_threads):
            t = threading.Thread(target = put_worker, args=(traversal, intermediate))
            t.daemon = True
            t.start()

        traversal.results.task_done()

def put_worker(traversal, intermediate):
    intermediate.process(traversal)

class WaitPair:
    def __init__(self, city, distance, wait, traversal):
        self.city = city
        self.distance = distance
        self.wait = wait
        self.update_comparisons = traversal.update_comparisons
        self.total_time = distance + wait

    def __cmp__(self, other):
        self.update_comparisons()
        return cmp(self.total_time, other.total_time)

    def __repr__(self):
        return '%s - Distance %d - Wait %d' % (self.city, self.distance, self.wait)


class IntermediateResult:
    def __init__(self, pair, cities_left, time_so_far, route_so_far):
        self.pair = pair
        self.cities_left = cities_left
        self.time_so_far = time_so_far
        self.route_so_far = route_so_far

    def __cmp__(self, other):
        if self.time_so_far == other.time_so_far:
            return cmp(len(self.cities_left), len(other.cities_left))
        return cmp(self.time_so_far, other.time_so_far)

    def __repr__(self):
        return "%s %d %s %s" % (self.pair.city, self.time_so_far, self.cities_left, self.route_so_far)

    def process(self, traversal):
        best_time = traversal.best_time
        time_so_far = self.time_so_far
        num_cities = len(self.cities_left)
        city = self.pair.city
        cities_left = self.cities_left
        route_so_far = self.route_so_far
        if traversal.compare(time_so_far + num_cities, best_time) <= 1:
            traversal.parallel_iterate(city, cities_left, time_so_far, route_so_far)
        pass


class Traversal:

    def __init__(self, start_day, start_city, cities, distances, wait_times, first_date_string, traverse_func_name):
        self.start_day = start_day
        self.start_city = start_city
        self.cities = cities
        self.distances = distances
        self.wait_times = wait_times
        self.first_date_string = first_date_string
        self.best_time = len(self.wait_times[start_city])
        self.best_route = []
        self.traversals = 0
        self.comparisons = 0
        self.threshold = 1000000
        self.comparisons_next = self.threshold
        self.log_comparisons = False
        self.log_traversals = False
        self.start_time = datetime.datetime.now()
        self.traverse_func = getattr(Traversal, traverse_func_name)
        self.results = Queue.PriorityQueue()
        self.done = False


        print 'Starting run at %s' % self.start_time

    def update_traversals(self):
        self.traversals = self.traversals + 1
        if (self.log_traversals and self.traversals % self.threshold == 0):
            print '%s: %d traversals' % (datetime.datetime.now(), self.traversals)

    def update_comparisons(self):
        self.comparisons = self.comparisons + 1
        if (self.log_comparisons and self.comparisons >= self.comparisons_next):
            print '%s: %d comparisons' % (datetime.datetime.now(), self.comparisons_next)
            self.comparisons_next = self.comparisons_next + self.threshold

    def compare(self, left, right):
        self.update_comparisons()
        return cmp(left, right)

    def do_iteration(self, pair, cities, time_so_far, route_so_far):
        last_city_distances = self.distances[pair.city]
        home_distance = last_city_distances[self.start_city]
        total_time = time_so_far + pair.distance + pair.wait
        if self.compare(total_time + home_distance, self.best_time) < 0:
            new_route = copy.copy(route_so_far)
            new_route.append(pair)
            new_time_so_far = total_time

            if self.compare(len(new_route), len(self.cities)) == 0:
                self.best_time = new_time_so_far + home_distance
                self.best_route = copy.copy(new_route)

                print '%s: SOLUTION FOUND - Traversals = %d, Comparisons = %d' % (datetime.datetime.now(), self.traversals, self.comparisons)
                print self.best_route
                print 'Home - %s - %d days' % (self.start_city, home_distance)
                print 'TOTAL - %d' % self.best_time
                sys.stdout.flush()

                return None
            else:
                new_cities = copy.copy(cities)
                new_cities.remove(pair.city)
                return IntermediateResult(pair, new_cities, new_time_so_far, new_route)
        else:
            return None

    def normal(self, city, cities, time_so_far, route_so_far):
        self.update_traversals()

        num_cities = len(cities)
        if self.compare(time_so_far + num_cities, self.best_time) > -1:
            return None

        city_distances = self.distances[city]
        total_distances_left = time_so_far
        pairs = []
        for new_city in cities:
            distance = city_distances[new_city]
            total_distances_left = total_distances_left + distance
            if self.compare(total_distances_left, self.best_time) > -1:
                continue
            city_wait_times = self.wait_times[new_city]
            wait_time = city_wait_times[time_so_far + distance]
            self.update_comparisons()
            if wait_time == 'x':
                continue
            wait_time = int(wait_time)
            pair = WaitPair(new_city, distance, wait_time, self)
            pairs.append(pair)

        for pair in pairs:
            intermediate = self.do_iteration(pair, cities, time_so_far, route_so_far)
            if intermediate:
                self.traverse_func(self, pair.city, intermediate.cities_left, intermediate.time_so_far, intermediate.route_so_far)
        pass



    def sorted(self, city, cities, time_so_far, route_so_far):
        self.update_traversals()

        num_cities = len(cities)
        if self.compare(time_so_far + num_cities, self.best_time) > -1:
            return None

        city_distances = self.distances[city]
        total_distances_left = time_so_far
        pairs = []
        for new_city in cities:
            distance = city_distances[new_city]
            total_distances_left = total_distances_left + distance
            if self.compare(total_distances_left, self.best_time) > -1:
                continue
            city_wait_times = self.wait_times[new_city]
            wait_time = city_wait_times[time_so_far + distance]
            self.update_comparisons()
            if wait_time == 'x':
                continue
            wait_time = int(wait_time)
            pair = WaitPair(new_city, distance, wait_time, self)
            pairs.append(pair)

        pairs.sort()

        for pair in pairs:
            intermediate = self.do_iteration(pair, cities, time_so_far, route_so_far)
            if intermediate:
                self.traverse_func(self, pair.city, intermediate.cities_left, intermediate.time_so_far, intermediate.route_so_far)

        pass

    def parallel_iterate(self, city, cities, time_so_far, route_so_far):
        self.update_traversals()

        num_cities = len(cities)
        if self.compare(time_so_far + num_cities, self.best_time) > -1:
            return None

        city_distances = self.distances[city]
        total_distances_left = time_so_far

        for new_city in cities:
            distance = city_distances[new_city]
            total_distances_left += distance
            if self.compare(total_distances_left, self.best_time) > -1:
                continue
            city_wait_times = self.wait_times[new_city]
            wait_time = city_wait_times[time_so_far + distance]
            self.update_comparisons()
            if wait_time == 'x':
                continue
            wait_time = int(wait_time)
            pair = WaitPair(new_city, distance, wait_time, self)
            intermediate = self.do_iteration(pair, cities, time_so_far, route_so_far)
            if intermediate:
                self.results.put(intermediate)

        pass

    def thread(self):
        for i in range(num_worker_threads):
            t = threading.Thread(target = get_worker, args=(self,))
            t.daemon = True
            t.start()
        time.sleep(1)
        self.results.join()
        pass


    def parallel(self, city, cities, time_so_far, route_so_far):
        pair = WaitPair(city, 0, 0, self)
        intermediate = IntermediateResult(pair, cities, time_so_far, route_so_far)
        self.results.put(intermediate)

        while not self.results.empty():
            self.thread()


    def start(self):
        self.traverse_func(self, self.start_city, self.cities, self.start_day, [])
        print '%s' % datetime.datetime.now()
        print 'Total traversals - %d' % traversal.traversals
        print 'Total comparisons - %d' % traversal.comparisons

city_list, global_distances = get_distances()
first_date_string, global_wait_times = get_wait_times()
start_city = args.start_city
start_day = args.start_day
dest_list = read_dest_list()
num_worker_threads = 2

traversal = Traversal(start_day, start_city, dest_list, global_distances, global_wait_times, first_date_string, args.traverse_style)
traversal.start()
