def test_import_basics() -> None:
    """
    This test will just import the attributes module and check if it exists.
    """

    try:
        import importlib.util
        imp = importlib.util.find_spec("attributes_dir.attributes", package="AttributesOriginal")
        found = imp is not None
        assert found
    except:
        assert False
    
def test_verify_each_original_attribute() -> None:
    """
    This test should verify all the different attributes. That they can be accessed and that they are strings.
    In this example we are only looking at the first attribute in AttributesOriginal.
    One should test all the attributes in each of the different subclasses of the attributes module.
    """
    
    from attributes_dir import attributes

    try:
        assert attributes.AttributesOriginal.accommodates.name == "accommodates"
    except:
        raise Exception("Could not import the original attribute accommodate from attributes")

    try:
        assert type(attributes.AttributesOriginal.accommodates.name) == str
    except:
        raise Exception(f"Accommodates from the attributes module is not of the type str, got {type(attributes.AttributesOriginal.accommodates.name)} instead")

def test_negative_each_original_attribute() -> None:
    """
    This is a negative test of AttributesOriginal. Here we will verify that we get the expected failure if we inject errors.
    Note that we need to add assert False in the try. Since, if the test passes and NOT goes direclty to the except while we are injecting the test has failed. 
    """
    
    from attributes_dir import attributes
    
    if not type(attributes.AttributesOriginal.accommodates.name) == str:
        assert False

    if not attributes.AttributesOriginal.accommodates.name == "accommodates":
        assert False
