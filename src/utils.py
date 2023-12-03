#%%
from functools import wraps

def safety_switch(input_type):

    def decor(func):

        @wraps(func)
        def wrapper(*args, **kwargs):
            
            allowed_types = set(input_type)

            input_to_check = [*args][1]
            
            if type(input_to_check) in allowed_types:
                
                try:
                    
                    result = func(*args, **kwargs)

                except Exception as e:
                    print(f"{func.__name__} Failed: ")
                    raise e


            else:     
                raise ValueError(f"INCORRECT INPUT! A {type(input_to_check)} was pushed. {func.__name__} only accepts {input_type} type(s)... If you need support on how to use AttributBoss, please review the documentation: https://dev.azure.com/gocloudclient0099/LPA/_git/AttributeBoss?path=%2FREADME.md&version=GBmain&_a=contents")   
                

            return result
        
        return wrapper
    
    return decor