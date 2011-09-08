<?php
include('squirrel.class.php');

$squirrel = new Squirrel();

$start = time();

for ($i = 0; $i < 10000; $i++)
	$squirrel->pop_head();

$end = time();
echo $end - $start;
?>