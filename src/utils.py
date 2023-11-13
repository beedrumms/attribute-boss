from functools import wraps

def safety_switch(input_type):

    def decor(func):

        @wraps(func)
        def wrapper(arg, *args, **kwargs):
            
            allowed_types = set(input_type)
            
            if type(arg) in allowed_types:
                
                try:
                    
                    result = func(arg, *args, **kwargs)

                except Exception as e:
                    print(func.__name__, " Failed: ")
                    raise e


            else: 
                print("INCORRECT INPUT")
                print(func.__name__, ' only accepts ', input_type)
                raise ValueError


            return result
        
        return wrapper
    
    return decor

