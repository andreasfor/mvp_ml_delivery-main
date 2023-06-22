def aggregate_reviews(review_scores_accuracy, review_scores_cleanliness, review_scores_checkin, review_scores_communication, review_scores_location, review_scores_value) -> float:

    """
    This function adds all hte review scores into one aggregated score.

    :return: An aggregated value of all the review scores.
    :rtype: float
    """

    aggregated_value = review_scores_accuracy + review_scores_cleanliness + review_scores_checkin + review_scores_communication + review_scores_location + review_scores_value

    return aggregated_value