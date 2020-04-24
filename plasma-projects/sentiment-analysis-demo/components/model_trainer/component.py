import tensorflow as tf
from tensorflow import keras
import logging

logger=logging.getLogger('model training')


class RNN(keras.Model):
    def __init__(self, units, num_classes, top_words, max_review_length):
        super(RNN, self).__init__()
        self.rnn = keras.layers.LSTM(units, return_sequences=True)
        self.rnn2 = keras.layers.LSTM(units)
        self.embedding = keras.layers.Embedding(
            top_words, 100, input_length=max_review_length)
        self.fc = keras.layers.Dense(1)

    def call(self, inputs, training=None, mask=None):
        x = self.embedding(inputs)
        x = self.rnn(x)
        x = self.rnn2(x)
        x = self.fc(x)
        return x


def model_creation(parameters):
    units = parameters["units"]
    num_classes = parameters["num_classes"]
    top_words = parameters["top_words"]
    max_review_length = parameters["max_review_length"]
    result = RNN(units, num_classes, top_words, max_review_length)
    return result


def model_training(parameters):
    model = model_creation(parameters)
    x_train = parameters["X_train"]
    y_train = parameters["y_train"]
    x_test = parameters["X_test"]
    y_test = parameters["y_test"]
    batch_size = parameters["batch_size"]
    epochs = parameters["epochs"]
    model.compile(
        optimizer=keras.optimizers.Adam(0.001),
        loss=keras.losses.BinaryCrossentropy(from_logits=True),
        metrics=['accuracy']
    )
    logging.info("training model")
    model.fit(
        x_train, y_train,
        batch_size=batch_size,
        epochs=epochs,
        validation_data=(x_test, y_test),
        verbose=1
    )
    logger.info("model training complete")
    output = {'model': model}
    return output


def main(args):
    output = model_training(args['parameters'])
    return output
