from tensorflow import keras
import logging

logger = logging.getLogger('data-fetcher')

def fetch_dataset(parameters):
    top_words = parameters['top_words']
    (X_train, y_train), (X_test, y_test) = keras.datasets.imdb.load_data(
        num_words=top_words)
    result = {
        "X_train": X_train,
        "y_train": y_train,
        "X_test": X_test,
        "y_test": y_test
    }
    logging.info("Completed the data fetching process")
    return result


def main(args):
    output = fetch_dataset(args['parameters'])
    return output
