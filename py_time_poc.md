time:
datetime(str, format= "%Y-%m-%d %H:%M:%S")->PyDateTime: 从字符串中解析日期时间
默认ymd hms, 也可以自定义格式
duration()->PyDuration: 从字符串中解析时间间隔
i.e: duration("1 day 2 hours 3 minutes 4 seconds")
duration("7d")
PyDuration.from_secs(86400)
