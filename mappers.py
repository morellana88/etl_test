# TODO figure out how to do it using python datetime or calendar libraries
dow_mapper={
    'Sunday': 0, 
    'Monday': 1, 
    'Tuesday': 2, 
    'Wednesday': 3, 
    'Thursday': 4, 
    'Friday': 5, 
    'Saturday': 6
}

def dow_converter(day):
    return int(dow_mapper.get(day,day))