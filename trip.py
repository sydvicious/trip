#!/usr/bin/env python

import copy
import math
import Queue

def get_distances():
    return_distances = {}

    with open('distances.txt', 'rU') as f:
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

    with open('schedule.txt', 'rU') as f:
        line = f.readline().strip()
        entries = line.split('\t')
        first_date_string = entries[1]
        cities_count = len(city_list)

        for city_index in xrange(0, cities_count):
            city = city_list[city_index]
            wait_times_raw = f.readline().strip().split('\t')[1:]
            wait_times = []
            for wait in wait_times_raw:
                if wait == 'x':
                    wait = 999
                wait_times.append(int(wait))
                return_wait_times[city] = wait_times
    return first_date_string, return_wait_times


def read_dest_list():
    with open('destinations.txt') as f:
        destinations = f.read().strip().split('\n')
    return set(destinations)

class WaitPair:
    def __init__(self, city, distance, wait, traversal):
        self.city = city
        self.distance = distance
        self.wait = wait
        self.traversal = traversal

    def __cmp__(self, other):
        self.traversal.update_comparisons()
        self_time = self.distance + self.wait
        other_time = other.distance + other.wait
        return cmp(self_time, other_time)

    def __repr__(self):
        return '%s - Distance %d - Wait %d' % (self.city, self.distance, self.wait)

class Traversal:
    def __init__(self, start_day, start_city, cities, distances, wait_times, first_date_string):
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

    def update_traversals(self):
        self.traversals = self.traversals + 1
        if (self.traversals % self.threshold == 0):
            print '%d traversals' % self.traversals

    def update_comparisons(self):
        self.comparisons = self.comparisons + 1
        if (self.comparisons >= self.comparisons_next):
            print '%d comparisons' % self.comparisons_next
            self.comparisons_next = self.comparisons_next + self.threshold

    def do_iteration(self, pair, cities, time_so_far, route_so_far, traverse_func):
        total_time = time_so_far + pair.distance + pair.wait
        self.update_comparisons()
        if total_time < self.best_time:
            new_route = copy.copy(route_so_far)
            new_route.append(pair)
            new_time_so_far = total_time
            if len(new_route) == len(self.cities):
                self.best_time = new_time_so_far
                self.best_route = copy.copy(new_route)
                last_city_distances = self.distances[pair.city]
                home_time = self.best_time + last_city_distances[self.start_city]
                print 'SOLUTION FOUND - Traversals = %d, Comparisons = %d' % (self.traversals, self.comparisons)
                print self.best_route
                print 'Home - %s - %d days' % (self.start_city, home_time)
            else:
                new_cities = copy.copy(cities)
                new_cities.remove(pair.city)
                traverse_func(pair.city, new_cities, new_time_so_far, new_route)
        else:
            return

    def traverse(self, city, cities, time_so_far, route_so_far):
        self.update_traversals()

        self.update_comparisons()
        num_cities = len(cities)
        if time_so_far + num_cities >= self.best_time:
            return

        city_distances = self.distances[city]
        total_distances_left = time_so_far
        pairs = []
        for new_city in cities:
            distance = city_distances[new_city]
            total_distances_left = total_distances_left + distance
            self.update_comparisons()
            if total_distances_left >= self.best_time:
                return
            city_wait_times = self.wait_times[new_city]
            wait_time = city_wait_times[time_so_far + distance]
            pair = WaitPair(new_city, distance, wait_time, self)
            pairs.append(pair)

        for pair in pairs:
            self.do_iteration(pair, cities, time_so_far, route_so_far, self.traverse)
        pass


    def traverse_djikstra(self, city, cities, time_so_far, route_so_far):
        self.update_traversals()

        self.update_comparisons()
        num_cities = len(cities)
        if time_so_far + num_cities >= self.best_time:
            return

        city_distances = self.distances[city]
        total_distances_left = time_so_far
        q = Queue.PriorityQueue()

        for new_city in cities:
            distance = city_distances[new_city]
            total_distances_left = total_distances_left + distance
            self.update_comparisons()
            if total_distances_left >= self.best_time:
                return

            city_wait_times = self.wait_times[new_city]
            wait_time = city_wait_times[time_so_far + distance]
            pair = WaitPair(new_city, distance, wait_time, self)
            q.put(pair)

        while not q.empty():
            pair = q.get()
            self.do_iteration(pair, cities, time_so_far, route_so_far, self.traverse_djikstra)
        pass

    def traverse_sort(self, city, cities, time_so_far, route_so_far):
        self.update_traversals()

        self.update_comparisons()
        num_cities = len(cities)
        if time_so_far + num_cities >= self.best_time:
            return

        city_distances = self.distances[city]
        total_distances_left = time_so_far
        pairs = []
        for new_city in cities:
            distance = city_distances[new_city]
            total_distances_left = total_distances_left + distance
            self.update_comparisons()
            if total_distances_left >= self.best_time:
                return
            city_wait_times = self.wait_times[new_city]
            wait_time = city_wait_times[time_so_far + distance]
            pair = WaitPair(new_city, distance, wait_time, self)
            pairs.append(pair)
        pairs.sort()

        for pair in pairs:
            self.do_iteration(pair, cities, time_so_far, route_so_far, self.traverse_sort)
        pass


    def start(self):
        self.traverse_sort(self.start_city, self.cities, self.start_day, [])

city_list, global_distances = get_distances()
first_date_string, global_wait_times = get_wait_times()
start_city = 'Bowling Green'
start_day = 0
dest_list = read_dest_list()
traversal = Traversal(start_day, start_city, dest_list, global_distances, global_wait_times, first_date_string)
traversal.start()
print ''
print 'Total traversals - %d' % traversal.traversals
print 'Total comparisons - %d' % traversal.comparisons
