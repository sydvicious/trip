#!/usr/bin/tclsh

package require Tcl 8.5

proc set_property {dictionary key value} {
    global $dictionary
    array set $dictionary [list $key $value]
}

proc get_property {dictionary key} {
    global $dictionary
    return [lindex [array get $dictionary $key] 1]
}

proc parse_args {} {
  global argc
  global argv
  global verbose
  global dest_file
  global schedule_file
  global distance_file
  global start_day
  global start_date
  global log_file

  set city "Austin"
  set start_day 0
  set verbose 0
  set log_file ""
  for {set i 0} {$i < $argc} {incr i} {
    set arg [lindex $argv $i]
    switch -exact -- $arg {
      "--city" {
	incr i
	set city [lindex $argv $i]
      }
      "--verbose" {
	set verbose 1
      }
      "--log-file" {
	incr i
	set log_file [lindex $argv $i]
      }
      "--start-day" {
	incr i
	set start_day [lindex $argv $i]
      }
      "--start-date" {
	incr i
	set start_date [lindex $argv $i]
      }
      "--destinations" {
	incr i
	set dest_file [lindex $argv $i]
      }
      "--schedule-file" {
	incr i
	set schedule_file [lindex $argv $i]
      }
      "--distance-file" {
	incr i
	set distance_file [lindex $argv $i]
      }
      default {
	error "Unknown argument $arg"
      }
    }
  }
  return $city
}

proc initialize {} {
  global city_names
  global schedule
  global schedule_file
  global distance_file
  global dates
  global dest_file
  global start_date
  global start_day
  global log_fd
  global verbose
  global log_file
  global num_entries
  
  set fd [open $distance_file]
  set line [split [gets $fd] \t]
  set city_count [lindex $line 0]
  set city_names [lrange $line 1 end]
  if {$city_count != [llength $city_names]} {
    error "Distances file has wrong number of cities."
  }
  
  if {$verbose} {
    if {[string equal $log_file ""]} {
      set log_fd [open "trip.log" w]
    } else {
      set log_fd [open $log_file w]
    }
  } else {
    set log_fd [open "/dev/null" w]
  }
  
  while {[gets $fd line] > 0} {
    set line [split $line \t]
    set city [lindex $line 0]
    set distances [lrange $line 1 end]
    for {set j 0} {$j < [llength $distances]} {incr j} {
      set dest_city [lindex $city_names $j]
      if {[string equal $city $dest_city]} {
	continue
      }
      set hours [lindex $distances $j]
      set days [::tcl::mathfunc::int [::tcl::mathfunc::ceil [expr ($hours - 4) / 8.0]]]
      set_property $city $dest_city $days
    }
  }
  close $fd
  
  set fd [open $schedule_file]

  set line [split [gets $fd] \t]
  set entries [lrange $line 1 end]
  set i 0
  foreach date $entries {
    set_property $i "date" $date
    set_property $date "day" $i
    incr i
  }
  
  while {[gets $fd line] > 0} {
    set entries [split $line \t]
    set city [lindex $entries 0]
    set entries [lrange $entries 1 end]
    set num_entries [llength $entries]
    for {set i 0} {$i < $num_entries} {incr i} {
      set entry [lindex $entries $i]
      set_property $city $i $entry
    }
  }
  
  close $fd
  
  set fd [open $dest_file]
  set target_cities {}
  while {[gets $fd line] > 0} {
    set line [string trim $line]
    if {![string equal $line ""]} {
      lappend target_cities $line
    }
  }
  close $fd

  set city_names [lsort $target_cities]

  if {[info exists start_date]} {
    set start_day [get_property $start_date "day"]
  }
}

proc remove_city_from_list {list city} {
  set index [lsearch $list $city]
  return [concat [lrange $list 0 [expr $index - 1]] [lrange $list [expr $index + 1] end]]
}

proc get_travel_time {from to} {
  return [get_property $from $to]
}

proc get_wait_time {from to day} {
  return [get_property $to [expr [get_travel_time $from $to] + $day]]
}

proc get_time {from to day} {
  global schedule
  global log_fd

  set travel_time [get_travel_time $from $to]
  set wait_time [get_wait_time $from $to $day]

  if {[string equal $wait_time ""]} {
    error "City $from not found in wait table."
  }

  if {[string equal $travel_time ""]} {
    error "City $from to found in travel table."
  } 
 
  if {[string equal $wait_time "x"]} {
    return "x"
  }

  return [expr $travel_time + $wait_time]
}

proc sort_cities_by_day_and_schedule {start day a b} {
  global schedule
  
  set a_time [get_time $start $a $day]
  set b_time [get_time $start $b $day]
  if {[string equal $a_time $b_time]} {
    return [string compare $a $b]
  } elseif {[string equal $a_time "x"]} {
    return 1;
  } elseif {[string equal $b_time "x"]} {
    return -1;
  } elseif {$a_time < $b_time} {
    return -1;
  } elseif {$b_time < $a_time} {
    return 1;
  }
  return 0;
}

proc log_traversals {route_so_far time_so_far next_time city next_city cities day} {
    global traversals
    global total_traversals
    global verbose
    global log_fd
    global best_time
    
    incr traversals
    incr total_traversals
    if {$traversals == 100000} {
        if {$verbose} {
            puts $log_fd "Iteration $total_traversals..."
            puts $log_fd "Best time so far - $best_time"
            puts $log_fd "Route so far - $route_so_far"
            puts $log_fd "Time so far - $time_so_far"
	    puts $log_fd "Next time - $next_time"
            puts $log_fd "Current city - $city"
	    puts $log_fd "Next city - $next_city"
            puts $log_fd "Cities left - $cities"
            puts $log_fd "Current day - $day"
            puts $log_fd ""
            flush $log_fd
        }
        set traversals 0
    }
}

proc traverse {city cities day time_so_far route_so_far} {
  global city_names
  global best_route
  global best_time
  global total_t
  global verbose
  global start_city
  global start_day
  global log_fd

  if {[catch {set sorted_cities [lsort -command "sort_cities_by_day_and_schedule [list $city] $day" $cities]} message]} {
    puts "Something went wrong - $message"
  } else {
  
    foreach next_city $sorted_cities {
      set time [get_time $city $next_city $day]
      log_traversals $route_so_far $time_so_far $time $city $next_city $sorted_cities $day

      if {[string equal $time "x"]} {
        continue
      }

      set new_time_so_far [expr $time_so_far + $time]

      set num_cities_left [expr [llength $cities] - 1]
      set time_left [expr $best_time - $new_time_so_far]

      if {$time_left < $num_cities_left} {
        continue
      }

      if {$new_time_so_far < $best_time} {
	set next_route $route_so_far
	lappend next_route [list $city $next_city [get_property $day "date"] Travel time: [get_travel_time $city $next_city] Wait time: [get_wait_time $city $next_city $day]]
	set next_cities [remove_city_from_list $cities $next_city]
	set next_day [expr $day + $time]
	if {[llength $next_cities] == 0} {
	  set home_time [get_property $next_city $start_city]
	  if {$next_day + $home_time < $best_time} {
	    # Success
            set best_route $next_route
	    lappend best_route [list $next_city $start_city [get_property $next_day "date"] Travel time: $home_time]
	    set best_time [expr $next_day + $home_time]
	    puts "FOUND - Iteration $total_traversals - [expr $best_time - $start_day] days - getting back on [get_property $best_time "date"]"
	    puts $log_fd "FOUND - Iteration $total_traversals - [expr $best_time - $start_day] days - getting back on [get_property $best_time "date"]"
	    foreach item $best_route {
	      puts "  $item"
	      puts $log_fd $item
	    }
	    puts ""
	    flush stdout
	    puts $log_fd ""
	    flush $log_fd
	    lappend best_route "Home on [get_property $best_time "date"]"
	  } else {
             continue
	  }
	} else {
	  traverse $next_city $next_cities $next_day $new_time_so_far $next_route
	}
      } else {
	# The list is sorted by time it takes, so we don't need to do further processing on this route.
        continue
      }
    }
  }
}

set start_city [parse_args]
initialize
set best_route {}
set best_time 82 
set traversals 0
set total_traversals 0
puts "Start City: $start_city"
set date [get_property $start_day "date"]
puts "Start Date: $date"
puts "Start Day: $start_day"
set cities [remove_city_from_list $city_names $start_city]
traverse $start_city $cities $start_day 0 {}
puts "Done."
foreach item $best_route {
  puts $item
}
puts "Total Traversals: $total_traversals"
if {$verbose} {
  close $log_fd
}

