import React, { useState } from 'react';
import {
    CInputGroup, CInputGroupText, CFormInput, CFormSelect,
    CButton, CModal, CModalBody, CModalHeader, CModalTitle, CModalFooter,
    CRow
} from '@coreui/react';
import './ConfigurationPanel.css';

const ConfigurationPanel = ({ promptType, setPromptType, numberOfShots, setNumberOfShots, handleGeneratePrompt, generatedPrompt }) => {
    const [showPromptPreview, setShowPromptPreview] = useState(false);

    const allowedPromptTypes = {
        "basic": 'Basic',
        "text_representation": 'Text Representation',
        "openai_demonstration": 'OpenAI Demonstration',
        "code_representation": 'Code Representation',
        "alpaca_sft": 'Alpaca SFT',
        "full_information": 'Full Information',
        "sql_only": 'SQL Only',
        "dail_sql": 'Dail SQL'
    };

    const handlePreviewClick = async () => {
        const isPromptGenerated = await handleGeneratePrompt();
        if (isPromptGenerated && generatedPrompt) {
            setShowPromptPreview(true);
        }
    };

    return (
        <div className="configuration-panel">
            <CRow>
                <h4>Prompt Configuration</h4>
            </CRow>
            <CRow>
                <CInputGroup className="mb-3">
                    <CInputGroupText as="label" htmlFor="promptType">Prompt Type</CInputGroupText>
                    <CFormSelect
                        onChange={(e) => {
                            setPromptType(e.target.value);
                            if (!["full_information", "sql_only", "dail_sql"].includes(e.target.value)) {
                                setNumberOfShots(0); 
                            }
                        }}
                    >
                        <option value="">Select Prompt Type</option>
                        {Object.entries(allowedPromptTypes).map(([key, value]) => (
                            <option key={key} value={key}>{value}</option>
                        ))}
                    </CFormSelect>
                </CInputGroup>

                <CInputGroup className="mb-3">
                    <CInputGroupText as="label" htmlFor="numberOfShots">Number of Shots</CInputGroupText>
                    <CFormInput
                        type="number"
                        value={numberOfShots}
                        min="1"
                        max="5"
                        onChange={(e) => setNumberOfShots(parseInt(e.target.value))}
                        disabled={!["full_information", "sql_only", "dail_sql"].includes(promptType)}
                    />
                </CInputGroup>
            </CRow>

            <CButton color="primary" onClick={handlePreviewClick}>
                Preview Prompt
            </CButton>

            <CModal className='prompt-preview-modal' visible={showPromptPreview} onClose={() => setShowPromptPreview(false)} size="lg" scrollable={true}>
                <CModalHeader>
                    <CModalTitle>Prompt Generated:</CModalTitle>
                </CModalHeader>
                <CModalBody>
                    {generatedPrompt}
                </CModalBody>
                <CModalFooter>
                    <CButton color="primary" onClick={() => setShowPromptPreview(false)}>Close</CButton>
                </CModalFooter>
            </CModal>
        </div>
    );
};

export default ConfigurationPanel;
