export const ERROR_MESSAGES = {
    SHOTS_NEGATIVE: "Shots must be a non-negative integer.",
    MAX_SHOTS_EXCEEDED: "Maximum number of shots possible is 5.",
    SHOTS_REQUIRED: "Number of shots must be greater than 0 for this prompt type.",
    PROMPT_TYPE_REQUIRED: "Please select a prompt type.",
    PROMPT_AND_TARGET_QUESTION_REQUIRED: "Please select a prompt type and enter a target question.",
    GENERATE_PROMPT_ERROR: "Error generating prompt. Please try again.",
    GENERATE_SQL_ERROR: "Error generating SQL. Please try again.",
    SCHEMA_CHANGE_ERROR: "Error Changing schema. Please try again.",
    FETCH_SCHEMA_ERROR: "Error Fetching Database Schema. Please try again.",
};

export const SUCCESS_MESSAGES = {
    SCHEMA_CHANGED_SUCCESS: "Schema Changed Successfully."
};
