import React, { useEffect, useState } from 'react';
import axios from 'axios';
import { CContainer, CRow, CCol, CToaster } from '@coreui/react';
import ConfigurationPanel from '../../components/ConfigurationPanel/ConfigurationPanel';
import ChatPanel from '../../components/ChatPanel/ChatPanel';
import ToastNotification from '../../components/ToastNotification/ToastNotification';
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
            showToast('Shots must be a non-negative integer.');
            return false;
        }
        if (numberOfShots > 5) {
            showToast('Maximum number of shots possible is 5.');
            setNumberOfShots(0);
            return false;
        }
        const requiresShots = ["full_information", "sql_only", "dail_sql"].includes(promptType);
        if (requiresShots && numberOfShots <= 0) {
            showToast('Number of shots must be greater than 0 for this prompt type.');
            return false;
        }
        return true;
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
            console.error('Error generating prompt:', err);
            const errorMessage = err.response?.data?.detail || 'Error generating prompt. Please try again.';
            showToast(errorMessage, 'error');
            return false;
        }
    };

    const handleGenerateAndExecuteQuery = async () => {
        if (!promptType) {
            showToast('Please select a prompt type.', 'error');
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
            console.error('Error generating SQL:', err);
            const errorMessage = err.response?.data?.detail?.error || 'Error generating SQL. Please try again.';
            showToast(errorMessage, 'error');
            
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
            showToast('Schema Changed Successfully', 'success');
            return true
        } catch (err) {
            console.error('Error Changing Database:', err);
            const errorMessage = err.response?.data?.detail || 'Error Changing schema. Please try again.';
            showToast(errorMessage, 'error');
            return false;
        }
    }

    const handlefetchSchema = async () => {
        try {
            const { data } = await axios.get('http://127.0.0.1:8000/database/schema');
            setDatabase(data);
            return true
        } catch (err) {
            console.error('Error Fetching Database Schema:', err);
            const errorMessage = err.response?.data?.detail || 'Error Fetching Database Schema. Please try again.';
            showToast(errorMessage);
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