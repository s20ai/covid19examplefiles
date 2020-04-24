from flask import jsonify, request, Flask
import tensorflow as tf
import numpy as np
from tensorflow import keras
import json
import os

model = None
app = Flask(__name__)
max_review_length = None


@app.route('/output', methods=['POST'])
def prediction_output():
    input = str(request.json['summary'])
    current_dir = os.getcwd()
    with open(current_dir + '/components/model_deployer' + '/imdb_word_index.json', 'r') as JSON:
        word2index = json.load(JSON)
        words = input.split()
        sequence = []
        for word in words:
            if word in word2index.keys():
                sequence.append(word2index[word])
            else:
                sequence.append(0)
        sentence = np.array([sequence])
        a = tf.convert_to_tensor(sentence, dtype=tf.float32)
        a = keras.preprocessing.sequence.pad_sequences(
            a, maxlen=max_review_length)
        output = model.predict(a)[0][0]
        result = {
            "input": input,
            "sentiment": str(output)
        }
    return jsonify(result)


def debug_server_init(parameters):
    global model
    model = parameters["model"]
    max_review_length = parameters["max_review_length"]
    app.run()


def main(args):
    output = debug_server_init(args['parameters'])
    return output
