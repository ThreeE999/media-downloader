import time
NOTICE = 35

def GetLogFileName(spider):
    current_timestamp = time.time()
    current_time_struct = time.localtime(current_timestamp)
    formatted_time = time.strftime("%Y-%m-%d %H:%M:%S", current_time_struct)
    return f"log/[{formatted_time}] {spider}.log"