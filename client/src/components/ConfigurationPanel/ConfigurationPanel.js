import React, { useState } from 'react';
import {
    CInputGroup, CInputGroupText, CFormInput, CFormSelect,
    CButton, CModal, CModalBody, CModalHeader, CModalTitle, CModalFooter,
    CRow
} from '@coreui/react';
import { ALLOWED_PROMPT_TYPES, NUMBER_OF_SHOTS_MAX, NUMBER_OF_SHOTS_MIN } from 'constants/promptEnums';
import './ConfigurationPanel.css';

const ConfigurationPanel = ({ promptType, setPromptType, numberOfShots, setNumberOfShots, handleGeneratePrompt, generatedPrompt, isFewShot }) => {
    const [showPromptPreview, setShowPromptPreview] = useState(false);

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
                            if (!isFewShot(e.target.value)) {
                                setNumberOfShots(0);
                            }
                        }}
                    >
                        <option value="">Select Prompt Type</option>
                        {Object.entries(ALLOWED_PROMPT_TYPES).map(([key, value]) => (
                            <option key={key} value={key}>{value}</option>
                        ))}
                    </CFormSelect>
                </CInputGroup>

                <CInputGroup className="mb-3">
                    <CInputGroupText as="label" htmlFor="numberOfShots">Number of Shots</CInputGroupText>
                    <CFormInput
                        type="number"
                        value={numberOfShots}
                        min={NUMBER_OF_SHOTS_MIN}
                        max={NUMBER_OF_SHOTS_MAX}
                        onChange={(e) => setNumberOfShots(parseInt(e.target.value))}
                        disabled={!isFewShot(promptType)}
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
