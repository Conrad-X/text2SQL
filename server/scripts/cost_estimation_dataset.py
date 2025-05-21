import os

from tqdm import tqdm
from utilities.config import PATH_CONFIG
from utilities.constants.services.llm_enums import ModelType
from utilities.constants.response_messages import (
    ERROR_PROCESSING_COST_ESTIMATION, WARNING_FILE_NOT_FOUND)
from utilities.cost_estimation import calculate_cost_and_tokens_for_file

# List of models to evaluate
MODELS = [
    ModelType.OPENAI_GPT3_5_TURBO_0125,
    ModelType.OPENAI_GPT4_0613,
    ModelType.OPENAI_GPT4_32K_0613,
    ModelType.OPENAI_GPT4_O,
    ModelType.OPENAI_GPT4_O_MINI
]

def calculate_cost_for_bird_dataset(model, is_batched):
    total_tokens = 0
    total_cost = 0.0
    results = []
    databases = [d for d in os.listdir(PATH_CONFIG.dataset_dir())]

    for db_name in tqdm(databases, desc=f"Processing {model} (Batched: {is_batched})"):
        db_name = os.path.splitext(db_name)[0] # remove .db in case we are working in synthetic data dir
        batch_input_file = PATH_CONFIG.batch_input_path(database_name=db_name)

        print(batch_input_file)

        # Skip if the batch input file does not exist
        if not os.path.exists(batch_input_file):
            tqdm.write(WARNING_FILE_NOT_FOUND.format(database_name=db_name))
            continue

        # Calculate cost and tokens for the current file
        try:
            tokens, cost, warnings = calculate_cost_and_tokens_for_file(
                batch_input_file, model, is_batched
            )
            total_tokens += tokens
            total_cost += cost

            results.append({
                "database": db_name,
                "tokens": tokens,
                "cost": cost,
                "warnings": warnings
            })

        except Exception as e:
            tqdm.write(ERROR_PROCESSING_COST_ESTIMATION.format(database_name=db_name, model=model, is_batched=is_batched, error=e))

    return results, total_tokens, total_cost

if __name__ == "__main__":
    """
    To run this script:

    1. Ensure you have set the correct `PATH_CONFIG.dataset_type` and `PATH_CONFIG.sample_dataset_type` in `utilities.config`:
       - Set `PATH_CONFIG.dataset_type` to DatasetType.BIRD_TRAIN for training data.
       - Set `PATH_CONFIG.dataset_type` to DatasetType.BIRD_DEV for development data.
       - Set `PATH_CONFIG.dataset_type` to DatasetType.SYNTHETIC for synthetic data.  

    2. Make sure you have generated batch input files:
        - Run the script `generate_batch_file` to prepare batch files for the chosen dataset.
        - In the terminal, navigate to the main project (server) folder and run:
            `python3 -m script.generate_batch_file`
    
    3. Run the cost estimation script:
        - After batch files are generated, navigate to the main project (server) folder.
        - In the terminal, run:
            `python3 -m script.cost_estimation_bird`

    Expected Output:
        - The script will iterate through each model, processing each database for both batched and non-batched modes.
        - A summary of total tokens and costs will be printed for each model and batching mode.
    """

    model_summaries = {}

    # Iterate over each model and calculate costs for both batched and non-batched modes
    for model in tqdm(MODELS, desc="Calculating costs for each model"):
        for is_batched in [True, False]:
            results, combined_tokens, combined_cost = calculate_cost_for_bird_dataset(model, is_batched)

            # Store results
            mode_key = f"{model.value} (Batched: {is_batched})"
            model_summaries[mode_key] = {
                "results": results,
                "total_tokens": combined_tokens,
                "total_cost": combined_cost
            }

    # Final summary
    print(f"\nSummary of Costs for Each Model and Batching Mode for {PATH_CONFIG.dataset_type.value}:")
    for mode, summary in model_summaries.items():
        print(f"\nModel: {mode}")
        print(f"Total Tokens: {summary['total_tokens']}")
        print(f"Total Cost: ${summary['total_cost']:.4f}")

