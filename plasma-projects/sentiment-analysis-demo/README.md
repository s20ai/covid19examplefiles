# Sentiment Analysis Workflow Demo

This project demonstrates how to implement a Plasma workflow to train and deploy
a sentiment analysis model.

## Components

The pipeline uses 5 components

- data_fetcher
- data_preprocessor
- model_trainer
- model_evaluator
- model_deployer

## Workflow

The workflow file is stored under the workflows/ directory.

## Usage

To run the project,

1. Clone this repository.
2. 'cd' into the cloned repository.
3. Run the following command to setup the project
```
	plasma project load sentiment-analysis-demo
```
4. Run the following commands to verify that the project got loaded

```
	plasma project component list
	plasma project workflow list
```
5. Run the following command to run the workflow

```
	plasma workflow run sentiment-analysis.yml
```

