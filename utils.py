from datetime import time ,datetime

def trim_single_quotes(text):
  if not text:
    return text 
  elif text[0] == "'" and text[-1] == "'":
    return text[1:-1] 
  else:
    return text  
  
def convert_to_time(time_str):
  try:
    # Split the string into hours and minutes
    hours, minutes = map(int, time_str.split(':'))
    # Create a time object
    return time(hour=hours, minute=minutes)
  except (ValueError, IndexError):
    raise ValueError(f"Invalid time format: {time_str}")
  

def convert_date_format(date_string: str) -> str:
    # Parse the input string to a datetime object
    date_obj = datetime.strptime(date_string, '%Y-%m-%d')
    
    # Format the datetime object to the desired string format
    formatted_date = date_obj.strftime('%Y-%m-%d')
    weekday = date_obj.strftime('%A')
    
    # Combine the formatted date and weekday
    result = f"{formatted_date}, {weekday}"
    
    return result