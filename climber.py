class Climber:

    def __init__(self, id :int , first_name :str, last_name :str, nationality :str, date_of_birth :str) -> None:
        self.id = id
        self.first_name = first_name
        self.last_name = last_name
        self.nationality = nationality
        self.date_of_birth = date_of_birth # time format
        
        
    def get_age() -> int:
        pass
    
    def get_expeditions() -> list:
        pass
        
    # Representation method
    # This will format the output in the correct order
    # Format is @dataclass-style: Classname(attr=value, attr2=value2, ...)
    def __repr__(self) -> str:
        return "{}({})".format(type(self).__name__, ", ".join([f"{key}={value!r}" for key, value in self.__dict__.items()]))
    
x = Climber(1, 'name1','last1', 'nl1', 'date')

print(repr(x))