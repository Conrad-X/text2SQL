import os
from tqdm import tqdm  # Progress bar
from utilities.cost_estimation import calculate_cost_and_tokens_for_file
from utilities.config import BATCH_INPUT_FILE_PATH, DATASET_DIR, DATASET_TYPE
from utilities.constants.LLM_enums import ModelType

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
    databases = [d for d in os.listdir(DATASET_DIR)]

    for db_name in tqdm(databases, desc=f"Processing {model} (Batched: {is_batched})"):
        db_name = os.path.splitext(db_name)[0] # remove .db in case we are working in synthetic data dir
        batch_input_file = BATCH_INPUT_FILE_PATH.format(database_name=db_name)

        print(batch_input_file)

        # Skip if the batch input file does not exist
        if not os.path.exists(batch_input_file):
            tqdm.write(f"Warning: File not found for {db_name}")
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
            tqdm.write(f"Error processing {db_name} for model {model} (Batched: {is_batched}): {str(e)}")

    return results, total_tokens, total_cost

if __name__ == "__main__":
    """
    To run this script:

    1. Ensure you have set the correct `DATASET_TYPE` in `utilities.config`:
        - Set `DATASET_TYPE` to DatasetType.BIRD_TRAIN for training data.
        - Set `DATASET_TYPE` to DatasetType.BIRD_DEV for development data.
        - Set `DATASET_TYPE` to DatasetType.SYNTHETIC for synthetic data.

    2. Generate batch input files:
        - Run the script `generate_batch_file.py` to prepare batch files for the chosen dataset.
        - In the terminal, navigate to the directory containing `generate_batch_file.py` and run:
          `python3 generate_batch_file.py`
    
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
    print(f"\nSummary of Costs for Each Model and Batching Mode for {DATASET_TYPE.value}:")
    for mode, summary in model_summaries.items():
        print(f"\nModel: {mode}")
        print(f"Total Tokens: {summary['total_tokens']}")
        print(f"Total Cost: ${summary['total_cost']:.4f}")

