import string
import random
from datetime import datetime, timedelta
lett = 'ЦУКЕНГШЩЗХФВАПРОЛДЖЭЯЧСМИТБЮ'
lettEnd = ['АЯ', 'ОЕ']
letters = string.ascii_uppercase
min_year = 1990
max_year = datetime.now().year

start = datetime(min_year, 1, 1, 00, 00, 00)
years = max_year - min_year + 1
end = start + timedelta(days=365 * years)
rand_string = ''.join(random.choice(letters) for i in range(1))