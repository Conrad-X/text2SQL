import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { CContainer, CRow, CCol, CToaster } from '@coreui/react';
import ConfigurationPanel from 'components/ConfigurationPanel/ConfigurationPanel';
import ChatPanel from 'components/ChatPanel/ChatPanel';
import ToastNotification from 'components/ToastNotification/ToastNotification';
import { ERROR_MESSAGES, SUCCESS_MESSAGES } from 'constants/messages';
import { PROMPT_TYPES } from 'constants/promptEnums';
import { TOAST_TYPE } from 'constants/toastType';
import './Text2SQL.css';

const Text2SQL = () => {
    const [promptType, setPromptType] = useState('');
    const [numberOfShots, setNumberOfShots] = useState(0);
    const [targetQuestion, setTargetQuestion] = useState('');

    const [generatedPrompt, setGeneratedPrompt] = useState('');
    const [database, setDatabase] = useState({});

    const [sqlQuery, setSqlQuery] = useState('');
    const [results, setResults] = useState([]);

    const [toastMessage, setToastMessage] = useState(null);

    const showToast = (message, type = 'error') => {
        setToastMessage(<ToastNotification message={message} type={type} onClose={() => setToastMessage(null)} />);
    };

    const validateShots = () => {
        if (isNaN(numberOfShots) || numberOfShots < 0) {
            showToast(ERROR_MESSAGES.SHOTS_NEGATIVE, TOAST_TYPE.ERROR);
            return false;
        }
        if (numberOfShots > 5) {
            showToast(ERROR_MESSAGES.MAX_SHOTS_EXCEEDED, TOAST_TYPE.ERROR);
            setNumberOfShots(0);
            return false;
        }
        if (isFewShot(promptType) && numberOfShots <= 0) {
            showToast(ERROR_MESSAGES.SHOTS_REQUIRED, TOAST_TYPE.ERROR);
            return false;
        }
        return true;
    };

    const isFewShot = (promptType) => {
        return [PROMPT_TYPES.FULL_INFORMATION, PROMPT_TYPES.SQL_ONLY, PROMPT_TYPES.DAIL_SQL].includes(promptType);
    };

    const handleGeneratePrompt = async () => {
        if (!validateShots()) return false;

        const questionToSend = targetQuestion || '{{ TARGET QUESTION }}';

        try {
            const { data } = await axios.post('http://127.0.0.1:8000/prompts/generate/', {
                prompt_type: promptType,
                shots: numberOfShots,
                question: questionToSend
            });

            setGeneratedPrompt(data.generated_prompt);
            return true
        } catch (err) {
            console.error(ERROR_MESSAGES.GENERATE_PROMPT_ERROR, err);
            const errorMessage = err.response?.data?.detail || ERROR_MESSAGES.GENERATE_PROMPT_ERROR;
            showToast(errorMessage, TOAST_TYPE.ERROR);
        }
    };

    const handleGenerateAndExecuteQuery = async () => {
        if (!promptType || !targetQuestion) {
            showToast(ERROR_MESSAGES.PROMPT_AND_TARGET_QUESTION_REQUIRED, TOAST_TYPE.ERROR);
            return;
        }

        if (!validateShots()) return;

        try {
            const { data } = await axios.post('http://127.0.0.1:8000/queries/generate-and-execute/', {
                prompt_type: promptType,
                shots: numberOfShots,
                question: targetQuestion
            });

            setGeneratedPrompt(data.prompt_used);
            setSqlQuery(data.query);
            setResults(data.result);
        } catch (err) {
            console.error(ERROR_MESSAGES.GENERATE_SQL_ERROR, err);
            const errorMessage = err.response?.data?.detail || ERROR_MESSAGES.GENERATE_SQL_ERROR;
            showToast(errorMessage, TOAST_TYPE.ERROR);

            setSqlQuery(err.response?.data?.detail?.query);
            setResults(err.response?.data?.detail?.result);
        }
    };

    const handleSchemaChange = async (databaseType) => {
        try {
            const { data } = await axios.post('http://127.0.0.1:8000/database/change', {
                database_type: databaseType
            });
            setDatabase(data);
            showToast(SUCCESS_MESSAGES.SCHEMA_CHANGED_SUCCESS, TOAST_TYPE.SUCCESS);
            return true
        } catch (err) {
            console.error(ERROR_MESSAGES.SCHEMA_CHANGE_ERROR, err);
            const errorMessage = err.response?.data?.detail || ERROR_MESSAGES.SCHEMA_CHANGE_ERROR;
            showToast(errorMessage, TOAST_TYPE.ERROR);
            return false;
        }
    }

    const handlefetchSchema = async () => {
        try {
            const { data } = await axios.get('http://127.0.0.1:8000/database/schema');
            setDatabase(data);
            return true
        } catch (err) {
            console.error(ERROR_MESSAGES.FETCH_SCHEMA_ERROR, err);
            const errorMessage = err.response?.data?.detail || ERROR_MESSAGES.FETCH_SCHEMA_ERROR;
            showToast(errorMessage, TOAST_TYPE.ERROR);
            return false;
        }
    }

    useEffect(() => {
        handlefetchSchema();
    }, [])

    return (
        <CContainer fluid className="text-2-sql">
            <CRow>
                <CCol sm={3}>
                    <ConfigurationPanel
                        promptType={promptType}
                        setPromptType={setPromptType}
                        numberOfShots={numberOfShots}
                        setNumberOfShots={setNumberOfShots}
                        handleGeneratePrompt={handleGeneratePrompt}
                        generatedPrompt={generatedPrompt}
                        handleSchemaChange={handleSchemaChange}
                        database={database}
                        isFewShot={isFewShot}
                    />
                </CCol>
                <CCol sm={9}>
                    <ChatPanel
                        handleGenerateAndExecuteQuery={handleGenerateAndExecuteQuery}
                        targetQuestion={targetQuestion}
                        setTargetQuestion={setTargetQuestion}
                        sqlQuery={sqlQuery}
                        results={results}
                    />
                </CCol>
            </CRow>

            <CToaster className="p-3" placement="top-end" push={toastMessage} />
        </CContainer>
    );
};

export default Text2SQL;
