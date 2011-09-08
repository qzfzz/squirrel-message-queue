<?php
/*
 * copyright(c)Liexusong 280259971@qq.com
 * Sina weibo @旭松_Lie
 * Date: 2011/09/13
 */

class Squirrel {
	private $sock;
	
	function Squirrel($host = '127.0.0.1', $port = 6061) {
		$this->sock = fsockopen($host, $port, $errno, $errstr, 5);
		if (!$this->sock) {
			exit('FATAL: Unable connect to server.');
		}
	}
	
	function auth($pwd) {
		fwrite($this->sock, sprintf("AUTH %s\r\n", $pwd));
		$retval = fgets($this->sock);
		if (preg_match("/#ERR:\s(.*)/", $retval, $match)) {
			$error = $match[1];
			return false;
		} else {
			return true;
		}
	}
	
	/* PUT First(PUTF) */
	function push_head($data, &$error = NULL) {
		fwrite($this->sock, sprintf("PUTF %d\r\n%s\r\n", strlen($data), $data));
		$retval = fgets($this->sock);
		if (preg_match("/#ERR:\s(.*)/", $retval, $match)) {
			$error = $match[1];
			return false;
		} else {
			return true;
		}
	}
	
	/* PUT Last(PUTL) */
	function push_tail($data, &$errno = NULL, &$error = NULL) {
		fwrite($this->sock, sprintf("PUTL %d\r\n%s\r\n", strlen($data), $data));
		$retval = fgets($this->sock);
		if (preg_match("/#ERR:\s(.*)/", $retval, $match)) {
			$error = $match[1];
		} else {
			return true;
		}
	}
	
	/* GET First(GETF) */
	function pop_head(&$errno = NULL, &$error = NULL) {
		$data = '';
		fwrite($this->sock, "POPF\r\n");
		while (($retval = fgets($this->sock))) {
			if (!strncmp($retval, '+END', 4)) break;
			if (preg_match("/#ERR:\s(.*)/", $retval, $match)) {
				$error = $match[1];
				return false;
			}
			if (preg_match("/\\$([0-9]+)/", $retval, $match)) {
				$wouldRecv = $match[1] + 2;//require "\r\n"
				while ($wouldRecv > 0) {
					$tmp .= fread($this->sock, $wouldRecv);
					$wouldRecv -= strlen($tmp);
					$data .= $tmp;
				}
			}
		}
		return substr($data, 0, -2);
	}
	
	/* GET Last(GETL) */
	function pop_tail(&$errno = NULL, &$error = NULL) {
		$data = '';
		fwrite($this->sock, "POPL\r\n");
		while (($retval = fgets($this->sock))) {
			if (!strncmp($retval, '+END', 4)) break;
			if (preg_match("/#ERR:\s(.*)/", $retval, $match)) {
				$error = $match[1];
				return false;
			}
			if (preg_match("/\\$([0-9]+)/", $retval, $match)) {
				$wouldRecv = $match[1] + 2;
				while ($wouldRecv > 0) {
					$tmp .= fread($this->sock, $wouldRecv);
					$wouldRecv -= strlen($tmp);
					$data .= $tmp;
				}
			}
		}
		return substr($data, 0, -2);
	}
	
	/* GET Position(GETP) */
	function pop_index($pos, &$errno = NULL, &$error = NULL) {
		$data = '';
		fwrite($this->sock, "POPF $pos\r\n");
		while (($retval = fgets($this->sock))) {
			if (!strncmp($retval, '+END', 4)) break;
			if (preg_match("/#ERR:\s(.*)/", $retval, $match)) {
				$error = $match[1];
				return false;
			}
			if (preg_match("/\\$([0-9]+)/", $retval, $match)) {
				$wouldRecv = $match[1] + 2;
				while ($wouldRecv > 0) {
					$tmp .= fread($this->sock, $wouldRecv);
					$wouldRecv -= strlen($tmp);
					$data .= $tmp;
				}
			}
		}
		return substr($data, 0, -2);
	}
	
	/* GET Some(GETS) */
	function pop_pile($nums, &$errno = NULL, &$error = NULL) {
		$retarray = array();
		fwrite($this->sock, "POPS $nums\r\n");
		while (($retval = fgets($this->sock))) {
			$data = '';
			if (!strncmp($retval, '+END', 4)) break;
			if (preg_match("/#ERR:\s(.*)/", $retval, $match)) {
				$error = $match[1];
				return false;
			}
			if (preg_match("/\\$([0-9]+)/", $retval, $match)) {
				$wouldRecv = $match[1] + 2;
				while ($wouldRecv > 0) {
					$tmp = fread($this->sock, $wouldRecv);
					$wouldRecv -= strlen($tmp);
					$data .= $tmp;
				}
				$retarray[] = substr($data, 0, -2);
			}
		}
		return $retarray;
	}
	
	/* GET First(GETF) */
	function get_head(&$errno = NULL, &$error = NULL) {
		$data = '';
		fwrite($this->sock, "GETF\r\n");
		while (($retval = fgets($this->sock))) {
			if (!strncmp($retval, '+END', 4)) break;
			if (preg_match("/#ERR:\s(.*)/", $retval, $match)) {
				$error = $match[1];
				return false;
			}
			if (preg_match("/\\$([0-9]+)/", $retval, $match)) {
				$wouldRecv = $match[1] + 2;//require "\r\n"
				while ($wouldRecv > 0) {
					$tmp .= fread($this->sock, $wouldRecv);
					$wouldRecv -= strlen($tmp);
					$data .= $tmp;
				}
			}
		}
		return substr($data, 0, -2);
	}
	
	/* GET Last(GETL) */
	function get_tail(&$errno = NULL, &$error = NULL) {
		$data = '';
		fwrite($this->sock, "GETL\r\n");
		while (($retval = fgets($this->sock))) {
			if (!strncmp($retval, '+END', 4)) break;
			if (preg_match("/#ERR:\s(.*)/", $retval, $match)) {
				$error = $match[1];
				return false;
			}
			if (preg_match("/\\$([0-9]+)/", $retval, $match)) {
				$wouldRecv = $match[1] + 2;
				while ($wouldRecv > 0) {
					$tmp .= fread($this->sock, $wouldRecv);
					$wouldRecv -= strlen($tmp);
					$data .= $tmp;
				}
			}
		}
		return substr($data, 0, -2);
	}
	
	/* GET Position(GETP) */
	function get_index($pos, &$errno = NULL, &$error = NULL) {
		$data = '';
		fwrite($this->sock, "GETF $pos\r\n");
		while (($retval = fgets($this->sock))) {
			if (!strncmp($retval, '+END', 4)) break;
			if (preg_match("/#ERR:\s(.*)/", $retval, $match)) {
				$error = $match[1];
				return false;
			}
			if (preg_match("/\\$([0-9]+)/", $retval, $match)) {
				$wouldRecv = $match[1] + 2;
				while ($wouldRecv > 0) {
					$tmp .= fread($this->sock, $wouldRecv);
					$wouldRecv -= strlen($tmp);
					$data .= $tmp;
				}
			}
		}
		return substr($data, 0, -2);
	}
	
	/* GET Some(GETS) */
	function get_pile($nums, &$errno = NULL, &$error = NULL) {
		$retarray = array();
		fwrite($this->sock, "GETS $nums\r\n");
		while (($retval = fgets($this->sock))) {
			$data = '';
			if (!strncmp($retval, '+END', 4)) break;
			if (preg_match("/#ERR:\s(.*)/", $retval, $match)) {
				$error = $match[1];
				return false;
			}
			if (preg_match("/\\$([0-9]+)/", $retval, $match)) {
				$wouldRecv = $match[1] + 2;
				while ($wouldRecv > 0) {
					$tmp = fread($this->sock, $wouldRecv);
					$wouldRecv -= strlen($tmp);
					$data .= $tmp;
				}
				$retarray[] = substr($data, 0, -2);
			}
		}
		return $retarray;
	}
	
	function size(&$errno = NULL, &$error = NULL) {
		$data = '';
		fwrite($this->sock, "SIZE\r\n");
		while (($retval = fgets($this->sock))) {
			if (!strncmp($retval, '+END', 4)) break;
			if (preg_match("/#ERR:\s(.*)/", $retval, $match)) {
				$error = $match[1];
				return false;
			}
			if (preg_match("/\\$([0-9]+)/", $retval, $match)) {
				$wouldRecv = $match[1] + 2;
				while ($wouldRecv > 0) {
					$tmp .= fread($this->sock, $wouldRecv);
					$wouldRecv -= strlen($tmp);
					$data .= $tmp;
				}
			}
		}
		if (preg_match("/SIZE\(([0-9]+)\)/", $data, $match)) {
			return $match[1];
		} else {
			return false;
		}
	}
	
	function stat(&$errno = NULL, &$error = NULL) {
		$data = '';
		fwrite($this->sock, "STAT\r\n");
		while (($retval = fgets($this->sock))) {
			if (!strncmp($retval, '+END', 4)) break;
			if (preg_match("/#ERR:\s(.*)/", $retval, $match)) {
				$error = $match[1];
				return false;
			}
			if (preg_match("/\\$([0-9]+)/", $retval, $match)) {
				$wouldRecv = $match[1] + 2;
				while ($wouldRecv > 0) {
					$tmp .= fread($this->sock, $wouldRecv);
					$wouldRecv -= strlen($tmp);
					$data .= $tmp;
				}
			}
		}
		return $data;
	}
	
	function slab(&$errno = NULL, &$error = NULL) {
		$data = '';
		fwrite($this->sock, "SLAB\r\n");
		while (($retval = fgets($this->sock))) {
			if (!strncmp($retval, '+END', 4)) break;
			if (preg_match("/#ERR:\s(.*)/", $retval, $match)) {
				$error = $match[1];
				return false;
			}
			if (preg_match("/\\$([0-9]+)/", $retval, $match)) {
				$wouldRecv = $match[1] + 2;
				while ($wouldRecv > 0) {
					$tmp .= fread($this->sock, $wouldRecv);
					$wouldRecv -= strlen($tmp);
					$data .= $tmp;
				}
			}
		}
		return $data;
	}
}
?>