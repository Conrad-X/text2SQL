import React, { useState } from 'react';
import { CInputGroup, CInputGroupText, CFormInput, CFormSelect, CButton, CRow, CCol, CCallout } from '@coreui/react';
import PreviewModal from '../PreviewModal/PreviewModal';
import './ConfigurationPanel.css';

const ConfigurationPanel = ({
    promptType, setPromptType, numberOfShots, setNumberOfShots, handleGeneratePrompt, generatedPrompt,
    handleSchemaChange, handlefetchSchema, database
}) => {
    const [showPromptPreview, setShowPromptPreview] = useState(false);
    const [showSchemaPreview, setShowSchemaPreview] = useState(false);
    const [databaseType, setDatabaseType] = useState('');

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

    const allowedDatabaseTypes = {
        "store": 'Store',
        "hotel": 'Hotel',
        "healthcare": 'Healthcare',
        "music_festival": 'Music Festival'
    };

    const handlePromptPreviewClick = async () => {
        const isPromptGenerated = await handleGeneratePrompt();
        if (isPromptGenerated && generatedPrompt) {
            setShowPromptPreview(true);
        }
    };

    const handleSchemaPreviewClick = async () => {
        if (database.schema) {
            setShowSchemaPreview(true);
        }
    };

    return (
        <div className="configuration-panel">

            <h4 className="mb-4">Prompt Configuration</h4>
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

            <CInputGroup className="mb-4">
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

            <h4 className="mb-4">Database Configuration</h4>
            <CInputGroup>
                <CInputGroupText as="label" htmlFor="databaseType">Database Type</CInputGroupText>
                <CFormSelect
                    onChange={(e) => setDatabaseType(e.target.value)}
                >
                    <option value="">Select Database</option>
                    {Object.entries(allowedDatabaseTypes).map(([key, value]) => (
                        <option key={key} value={key}>{value}</option>
                    ))}
                </CFormSelect>
            </CInputGroup>
            <CButton className="w-50 ms-auto mb-2 mt-2" color="primary" onClick={() => handleSchemaChange(databaseType)} disabled={!databaseType || database.database_type == databaseType}>
                Change Database
            </CButton>

            <div className="preview-buttons">
                <CRow>
                    <CCol >
                        <CCallout color="primary" className="p-3">
                            Current Schema: <strong>{allowedDatabaseTypes[database.database_type]}</strong>
                        </CCallout>
                    </CCol>
                </CRow>
                <CButton color="primary" onClick={handlePromptPreviewClick} disabled={!promptType} >
                    Preview Prompt
                </CButton>
                <CButton color="primary" onClick={handleSchemaPreviewClick}>
                    Preview Database
                </CButton>
            </div>

            <PreviewModal
                title="Prompt Generated:"
                content={generatedPrompt}
                show={showPromptPreview}
                setShow={setShowPromptPreview}
            />

            <PreviewModal
                title="Database Schema:"
                content={database.schema}
                show={showSchemaPreview}
                setShow={setShowSchemaPreview}
            />
        </div >
    );
};

export default ConfigurationPanel;
