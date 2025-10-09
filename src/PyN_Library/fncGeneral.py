






import random
import string







def get_short_uid(length=8):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(length))