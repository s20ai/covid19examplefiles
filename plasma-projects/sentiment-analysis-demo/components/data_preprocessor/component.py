from tensorflow import keras
import logging

logger = logging.getLogger('data_preprocessing')

def preprocessing(parameters):
    logger.info('preprocessing data')
    max_review_length = parameters['max_review_length']
    X_train = parameters["X_train"]
    X_test = parameters["X_test"]
    x_train = keras.preprocessing.sequence.pad_sequences(
        X_train, maxlen=max_review_length)
    x_test = keras.preprocessing.sequence.pad_sequences(
        X_test, maxlen=max_review_length)
    result = {
        "X_train": x_train,
        "X_test": x_test
    }
    return result


def main(args):
    output = preprocessing(args['parameters'])
    return output
