class Clase:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
       
        
if __name__ == "__main__":
    obj = Clase(a=23,b=345,c=2342,d=4234)
    print(obj)
    print(obj.a)