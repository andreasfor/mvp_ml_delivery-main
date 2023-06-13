from enum import Enum, auto

from medallion_dir import medallion as M
from medallion_dir import imedallion as IM


class MedallionFactory():
    """Factory Method is a Creational Design Pattern that allows an interface or a class to create an object, but lets subclasses decide which class or object to instantiate. Using the Factory method, we have the best ways to create an object. Here, objects are created without exposing the logic to the client, and for creating the new type of object, the client uses the same common interface. source_ https://www.geeksforgeeks.org/factory-method-python-design-patterns/ .
    
    The first version only reads from internal database
    """

    class Version(Enum):
        V1 = auto()


    def create_or_get(**kwargs) -> M.Medallion:
        """Creates an object which the IMedallion functionality can be accessed"""
        
        try:
            if not isinstance(kwargs["version"], MedallionFactory.Version):
                raise IncorrectSpecificationError(f"Argument 'version' is not a Factory.Version, instead we "
                                                    f"got {kwargs['version']}")
        except NameError:
            raise IncorrectSpecificationError(f"No argument 'version' provided")

        try:
            if not isinstance(kwargs["call"], IM.IMedallion.Call):
                raise IncorrectSpecificationError(f"Argument 'call' is not a IMedallion.Call, instead we "
                                                    f"got {kwargs['call']}")
        except NameError:
            raise IncorrectSpecificationError(f"No argument 'call' provided")

        if kwargs["version"] == MedallionFactory.Version.V1:
            if kwargs["call"] == IM.IMedallion.Call.RAW_INTERNAL_DATABASE:
                return M.Medallion()