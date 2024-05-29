from datetime import time

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