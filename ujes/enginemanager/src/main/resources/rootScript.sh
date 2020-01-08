#!/usr/bin/expect -f

set user [lindex $argv 0]  #第一个参数  uernaem
set command [lindex $argv 1] # 第二个参数 sparksubmit 或则java -jar 命令
set timeout -1

spawn sudo su -
expect "~]# "
send "su - $user\r"
expect "~]* "
send "$command \r"
expect "~]* "
send "exit\r"
expect "~]# "
send "exit\r"
expect "~]$ "