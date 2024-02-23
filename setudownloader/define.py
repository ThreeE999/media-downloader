import time

NOTICE = 35
NOTICE_WARN = 36

def GetLogFileName(spider):
    current_timestamp = time.time()
    current_time_struct = time.localtime(current_timestamp)
    formatted_time = time.strftime("%Y_%m_%d_%H_%M", current_time_struct)
    return f"log/{formatted_time}_{spider}.log"