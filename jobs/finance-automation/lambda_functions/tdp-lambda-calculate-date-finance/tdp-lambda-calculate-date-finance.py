from datetime import datetime, timedelta
import calendar

def lambda_handler(event, context):
    today = datetime.now()
    
    first_day_of_current_month = today.replace(day=1)
    
    _, last_day = calendar.monthrange(today.year, today.month)
    last_day_of_current_month = today.replace(day=last_day)
    
    start_date = first_day_of_current_month.strftime('%Y-%m-%d')
    end_date = last_day_of_current_month.strftime('%Y-%m-%d')
    
    return {
        'statusCode': 200,
        'body': {
            'start_date': start_date,
            'end_date': end_date
        }
    }