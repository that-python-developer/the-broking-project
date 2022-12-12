def uppercase_decorator(function):
    def wrapper():
        func = function()
        upper_case = func.upper()
        return upper_case
    return wrapper


def split_string(function):
    def wrapper():
        func = function()
        splitted_string = func.split()
        return splitted_string
    return wrapper


@split_string
@uppercase_decorator
def say_hi():
    return 'hello there'
print(say_hi())