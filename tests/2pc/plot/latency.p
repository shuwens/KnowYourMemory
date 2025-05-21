filelist=system("ls data/sr_test_lat_send*.csv")
filenr=system("ls data/sr_test_lat_send*.csv | wc -l")

set terminal png small size 960,(filenr+1)/2*360 enhanced
set output "plot/latency.png"
set multiplot layout (filenr+1)/2,2 rowsfirst

set xlabel "Latency ({/Symbol u}s)" enhanced
unset key

do for [filename in filelist] {
  stats filename using 0 nooutput
  set yrange [*:*]
  set ytics 0,.2,1.3
  set format y "%10.1f"

  stats filename using 1 nooutput prefix "D"
  summary = sprintf("median: %d {/Symbol u}s \nmax: %d {/Symbol u}s\nmin: %d {/Symbol u}s", D_median, D_max, D_min)
  set label 1 summary at D_max/2., .5

  set title filename noenhanced 
  plot filename using ($1):(1./STATS_max) smooth cumul 
  unset label 1
}
