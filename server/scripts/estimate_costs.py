import json
from concurrent.futures import as_completed
from collections import defaultdict
import concurrent
from tqdm import tqdm

from app import db
from utilities.vectorize import make_samples_collection
from utilities.logging_utils import setup_logger
from utilities.config import PATH_CONFIG
from utilities.llm_metrics.token_calculator import TokenCalculator
from utilities.constants.LLM_enums import LLMType, ModelType
from utilities.constants.prompts_enums import FormatType, PromptType, RefinerPromptType
from utilities.prompts.prompt_factory import PromptFactory
from utilities.sql_improvement import generate_refiner_chat, generate_refiner_prompt
from utilities.candidate_selection import get_candidate_selector_prompt

logger = setup_logger(__name__)

MAX_WORKERS = 10

AVG_SQL_QUERY_TOKENS = 60
AVG_SQL_RESULT_ROW_TOKENS = 2

def process_item(item, config_options, selector_model, current_database):
    total_cost = 0
    sqls_with_config = []
    config_costs = {}   # {config_id: (cost, input_tokens, outputtokens)}
    model_costs = {}    # {model_key: (cost, input_tokens, outputtokens)}

    for config in config_options:
        config_cost = 0
        input_config_tokens = 0
        output_config_tokens = 0

        # --- Main prompt generation ---
        calculator = TokenCalculator(config['model'][1], config['model'][0])
        prompt = PromptFactory.get_prompt_class(
            prompt_type=config['prompt_config']['type'],
            target_question=item["question"],
            shots=config['prompt_config']['shots'],
            schema_format=config['prompt_config']['format_type'],
            schema=item['runtime_schema_used'] if config['prune_schema'] else None,
            evidence=item['evidence'] if config['add_evidence'] else None,
        )

        input_prompt_tokens = calculator.calculate_tokens_for_prompt(prompt)
        output_prompt_tokens = AVG_SQL_QUERY_TOKENS

        prompt_cost = calculator.calculate_cost(input_prompt_tokens, output_prompt_tokens)

        # Update per config costs
        config_cost += prompt_cost
        input_config_tokens += input_prompt_tokens
        output_config_tokens += output_prompt_tokens

        # Update per model costs
        model_key = f"{config['model'][0].name}_{config['model'][1].name}"
        prev_cost, prev_input_tokens, prev_output_tokens = model_costs.get(model_key, (0, 0, 0))
        model_costs[model_key] = (prev_cost + prompt_cost, prev_input_tokens + input_prompt_tokens, prev_output_tokens + output_prompt_tokens)

        # --- Improvement phase  ---
        if config.get('improve'):
            improver_calculator = TokenCalculator(config['improve']['client'][1], config['improve']['client'][0])
            max_improve = config['improve']['max_attempts']

            if config['improve']['chat_mode']:
                refiner_chat = []
                input_refiner_tokens = 0
                for _ in range(max_improve):
                    refiner_chat += generate_refiner_chat(
                        pred_sql="",
                        results="",
                        target_question=item["question"],
                        shots=config['improve']['shots'],
                        schema_used=item['runtime_schema_used'] if config['prune_schema'] else None,
                        evidence=item['evidence'] if config['add_evidence'] else None,
                        refiner_prompt_type=config['improve']['prompt'],
                        chat=refiner_chat
                    )
                    input_refiner_tokens += improver_calculator.calculate_tokens_for_chat(refiner_chat)

            else:
                input_refiner_tokens = 0
                for _ in range(max_improve):
                    refiner_prompt = generate_refiner_prompt(
                        pred_sql="",
                        results="",
                        target_question=item["question"],
                        shots=config['improve']['shots'],
                        schema_used=item['runtime_schema_used'] if config['prune_schema'] else None,
                        evidence=item['evidence'] if config['add_evidence'] else None,
                        refiner_prompt_type=config['improve']['prompt'],
                    )
                    input_refiner_tokens += improver_calculator.calculate_tokens_for_prompt(refiner_prompt)

            input_refiner_tokens += max_improve * (AVG_SQL_QUERY_TOKENS + (AVG_SQL_RESULT_ROW_TOKENS * 5)) # Adding the predicted sql and its results tokens
            output_refiner_tokens = AVG_SQL_QUERY_TOKENS

            refiner_cost = calculator.calculate_cost(input_refiner_tokens, output_refiner_tokens)

            # Update per config costs
            config_cost += refiner_cost
            input_config_tokens += input_refiner_tokens
            output_config_tokens += output_refiner_tokens

            # Update per model costs
            prev_cost, prev_input_tokens, prev_output_tokens = model_costs.get(model_key, (0, 0, 0))
            model_costs[model_key] = (prev_cost + refiner_cost, prev_input_tokens + input_refiner_tokens, prev_output_tokens + output_refiner_tokens)

        # Update total and total config costs
        config_id = config['config_id']
        prev_cost, prev_input_tokens, prev_output_tokens = config_costs.get(config_id, (0, 0, 0))
        config_costs[config_id] = (prev_cost + config_cost, prev_input_tokens + input_config_tokens, prev_output_tokens + output_config_tokens)
        total_cost += config_cost
        
        # Add the Dummy SQL and res to the list
        sqls_with_config.append(("", config_id, ""))
        
    # --- Selection phase ---
    if len(sqls_with_config) > 1:
        selector_calculator = TokenCalculator(selector_model['model'][1], selector_model['model'][0])
        selection_prompt, _, _, _ = get_candidate_selector_prompt(
            sqls_with_config,
            item["question"],
            current_database,
            item['runtime_schema_used'] if config['prune_schema'] else None,
            item['evidence'] if config['add_evidence'] else None
        )

        # Update total costs
        input_selection_tokens = selector_calculator.calculate_tokens_for_prompt(selection_prompt)
        input_selection_tokens += len(sqls_with_config) * ((AVG_SQL_RESULT_ROW_TOKENS * 10) + AVG_SQL_QUERY_TOKENS) # Adding the candidate sqls and their results tokens
        output_selection_tokens = 1 # Only returns a letter 

        selection_cost = selector_calculator.calculate_cost(input_selection_tokens, output_selection_tokens)

        total_cost += selection_cost

        # Update per model costs
        model_key = f"{selector_model['model'][0].name}_{selector_model['model'][1].name}"
        prev_cost, prev_input_tokens, prev_output_tokens = model_costs.get(model_key, (0, 0, 0))
        model_costs[model_key] = (prev_cost + selection_cost, prev_input_tokens + input_selection_tokens, prev_output_tokens + output_selection_tokens)

    return total_cost, config_costs, model_costs

def calculate_total_cost(selector_model, config_options, test_file_path):
    global_total_cost = 0
    global_config_costs = {}   # {config_id: (cost, input_tokens, outputtokens)}
    global_models_cost = {}    # {model_key: (cost, input_tokens, outputtokens)}

    logger.info("Loading test data from file: %s", test_file_path)
    with open(test_file_path, "r") as file:
        test_data = json.load(file)[:2]

    # Group items by db_id
    db_groups = defaultdict(list)
    for item in test_data:
        db_groups[item["db_id"]].append(item)

    # Create samples collection for few shot prompts
    if any(config['prompt_config']['shots'] > 0 for config in config_options):
        make_samples_collection()

    # Process each database separately
    for database, items in db_groups.items():
        db.set_database(database)
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_item = {executor.submit(process_item, item, config_options, selector_model, database): item for item in items}
            
            for future in tqdm(as_completed(future_to_item), total=len(items),
                               desc=f"Processing questions for database {database}"):
                try:
                    local_total_cost, local_config, local_models = future.result()

                    # Update total cost
                    global_total_cost += local_total_cost

                    # Update config cost
                    for config_id, (cost, input_tokens, output_tokens) in local_config.items():
                        prev_cost, prev_input_tokens, prev_output_tokens = global_config_costs.get(config_id, (0, 0, 0))
                        global_config_costs[config_id] = (prev_cost + cost, prev_input_tokens + input_tokens, prev_output_tokens + output_tokens)

                    # Update models cost
                    for model, (cost, input_tokens, output_tokens) in local_models.items():
                        prev_cost, prev_input_tokens, prev_output_tokens = global_models_cost.get(model, (0, 0, 0))
                        global_models_cost[model] = (prev_cost + cost, prev_input_tokens + input_tokens, prev_output_tokens + output_tokens)

                except Exception as exc:
                    logger.error("Generated an exception: %s", exc)

    return global_total_cost, global_config_costs, global_models_cost

if __name__ == "__main__":
    """
    To run this script:

    1. Ensure you have set the correct DATASET_TYPE and SAMPLE_DATASET_TYPE in .env:
        - Set DATASET_TYPE to 'bird_train', 'bird_dev' or 'bird_test'
        - Set SAMPLE_DATASET_TYPE to to 'bird_train' or 'bird_dev'

    2. Adjust Input Variables:
        - Ensure all input variables, such as LLM configurations, and prompt configurations, are correctly defined.
        - To enable improvement update each configs 'improve' key with the improver model, prompt_type (xiyan or basic), shots and max attempts to improve
        - To disable improvement set 'improve' to False or None.
        - To use pruned schema set 'prune_schema' to True
        - To use evidence in the prompts set 'add_evidence' to True

    3. Expected Outputs:
        - Total cost: The total estimated cost of processing all items.
        - Total for config {config_id}: Cost, Input Tokens, and Output Tokens for each configuration.
        - Total for model {model}: Cost, Input Tokens, and Output Tokens for each model.
    """

    selector_model = {
        "model": [LLMType.GOOGLE_AI, ModelType.GOOGLEAI_GEMINI_2_0_FLASH_THINKING_EXP_0121],
        "temperature": 0.2,
        "max_tokens": 8192,
    }

    config_options = [
        {
            'config_id': 1,
            "model": [LLMType.DASHSCOPE, ModelType.DASHSCOPE_QWEN_MAX],
            "temperature": 0.2,
            "max_tokens": 8192,
            "prompt_config": {
                "type": PromptType.SEMANTIC_FULL_INFORMATION,
                "shots": 7,
                "format_type": FormatType.M_SCHEMA,
            },
            "improve": {
                "client": [LLMType.GOOGLE_AI, ModelType.GOOGLEAI_GEMINI_2_0_FLASH],
                "prompt": RefinerPromptType.XIYAN,
                "max_attempts": 2,
                'shots': 5,
                "chat_mode": True
            },
            "prune_schema": True,
            "add_evidence": True,
        },
        {
            'config_id': 2,
            "model": [LLMType.OPENAI, ModelType.OPENAI_GPT4_O_MINI],
            "temperature": 0.2,
            "max_tokens": 8192,
            "prompt_config": {
                "type": PromptType.SEMANTIC_FULL_INFORMATION,
                "shots": 7,
                "format_type": FormatType.M_SCHEMA,
            },
            "improve": {
                "client": [LLMType.GOOGLE_AI, ModelType.GOOGLEAI_GEMINI_2_0_FLASH],
                "prompt": RefinerPromptType.XIYAN,
                "max_attempts": 2,
                'shots': 5,
                "chat_mode": False
            },
            "prune_schema": True,
            "add_evidence": True,
        },
        {
            'config_id': 3,
            "model": [LLMType.DEEPSEEK, ModelType.DEEPSEEK_CHAT],
            "temperature": 0.2,
            "max_tokens": 8192,
            "prompt_config": {
                "type": PromptType.TEXT_REPRESENTATION,
                "shots": 7,
                "format_type": FormatType.M_SCHEMA,
            },
            "improve": {
                "client": [LLMType.GOOGLE_AI, ModelType.GOOGLEAI_GEMINI_2_0_FLASH],
                "prompt": RefinerPromptType.XIYAN,
                "max_attempts": 2,
                'shots': 5,
                "chat_mode": False
            },
            "prune_schema": True,
            "add_evidence": True,
        }
    ]

    processed_test_file_path = PATH_CONFIG.processed_test_path(global_file=True)

    global_total_cost, global_config_costs, global_models_cost = calculate_total_cost(selector_model, config_options, processed_test_file_path)
    
    print("-"*60,f"Total cost: ${global_total_cost:.6f}\n", "-"*60)

    # Print costs for each configuration
    print("Costs by Configuration: \n", "-"*60)
    print(f"{'Config ID':<15}\t{'Cost':<15}\t{'Input Tokens':<15}\t{'Output Tokens':<15}")
    for config_id, (cost, input_tokens, output_tokens) in global_config_costs.items():
        print(f"{config_id:<15}\t${cost:<13.6f}\t{input_tokens:<15}\t{output_tokens:<15}")

    print("\n", "-"*60)

    # Print costs for each model
    print("Costs by Model: \n", "-"*60)
    print(f"{'Model':<15}\t{'Cost':<15}\t{'Input Tokens':<15}\t{'Output Tokens':<15}")
    for model, (cost, input_tokens, output_tokens) in global_models_cost.items():
        print(f"{model:<15}\t${cost:<13.6f}\t{input_tokens:<15}\t{output_tokens:<15}")