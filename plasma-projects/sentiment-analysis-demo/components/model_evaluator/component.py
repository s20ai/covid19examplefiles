import tensorflow as tf
import logging

logger = logging.getLogger('model_evaluator')

def evaluate(parameters):
    try:
        logger.info('evaluating model loss')
        X_test = parameters["X_test"]
        y_test = parameters["y_test"]
        batch_size = parameters["batch_size"]
        model = parameters["model"]
        result = model.evaluate(X_test, y_test, batch_size, verbose=1)
        logger.info('model loss : '+str(result))
        return True
    except Exception as e:
        logger.error(e)
        


def main(args):
    output = evaluate(args['parameters'])
    return output
