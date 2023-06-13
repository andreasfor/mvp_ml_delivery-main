from enum import Enum, auto

class AttributesOriginal(Enum):
    host_is_superhost = auto()
    cancellation_policy = auto()
    instant_bookable = auto()
    host_total_listings_count = auto()
    neighbourhood_cleansed = auto()
    latitude = auto()
    longitude = auto()
    property_type = auto()
    room_type = auto()
    accommodates = auto()
    bathrooms = auto()
    bedrooms = auto()
    beds = auto()
    bed_type = auto()
    minimum_nights = auto()
    number_of_reviews = auto()
    review_scores_rating = auto()
    review_scores_accuracy = auto()
    review_scores_cleanliness = auto()
    review_scores_checkin = auto()
    review_scores_communication = auto()
    review_scores_location = auto()
    review_scores_value = auto()
    

class AttributesAdded(Enum):
    aggregated_review_scores = auto()


class AttributesTarget(Enum):
    price = auto()
    

class TableNames:
    raw_airbnb = "default.airbnb"
    gold_tbl = "default.gold_tbl"
    
    test_df_simulate_daily_inserts = "default.test_df_simulate_daily_inserts"
    skewed_test_df_simulate_daily_inserts = "default.skewed_test_df_simulate_daily_inserts"
    skewed_test_df_simulate_daily_inserts_cleaned = "default.skewed_test_df_simulate_daily_inserts_cleaned"
    unseen_data_passed_to_model = "default.unseen_data_passed_to_model"

    reference_data_data_drift_train_data_only = "default.reference_data_data_drift_train_data_only"
    
    data_drift_df = "default.data_drift_df"
    model_drift_df = "default.model_drift_df"

    daily_pred_df = "default.daily_pred_df"