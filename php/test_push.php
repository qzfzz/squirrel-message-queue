<?php
include('squirrel.class.php');

$squirrel = new Squirrel();

$start = time();

for ($i = 0; $i < 100000; $i++)
	$squirrel->push_tail("This is SquirrelMQ TEST, This is number[$i] record.");

$end = time();
echo $end - $start;
?>
