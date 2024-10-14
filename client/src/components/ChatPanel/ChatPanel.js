import React from 'react';
import { CButton, CCard, CCardBody, CForm, CFormInput, CTable, CTableHead, CTableBody, CTableRow, CTableHeaderCell, CTableDataCell } from '@coreui/react';
import './ChatPanel.css';

const ChatPanel = ({ handleGenerateAndExecuteQuery, targetQuestion, setTargetQuestion, sqlQuery, results }) => {
    return (
        <CCard className="chat-panel">
            <CCardBody className="chat-panel d-flex flex-column">

                <div className="mt-4">
                    <h5>Type your natural language question here</h5>
                    <CForm className="d-flex">
                        <CFormInput
                            type="text"
                            value={targetQuestion}
                            onChange={(e) => setTargetQuestion(e.target.value)}
                            placeholder="Enter your NLP question"
                            className="me-2"
                        />
                        <CButton color="primary" onClick={handleGenerateAndExecuteQuery}>Submit</CButton>
                    </CForm>
                </div>

                <div className="mt-4">
                    <h5>Generated SQL:</h5>
                    {sqlQuery ? (
                        <div className="p-2 rounded bg-light">{sqlQuery}</div>
                    ) : (
                        <div className={`p-2 rounded bg-light text-muted`}>No SQL query generated yet.</div>
                    )}


                </div>

                <div className="mt-4">
                    <h5>Results:</h5>
                    {results.length > 0 ? (
                        <CTable striped bordered responsive>
                            <CTableHead>
                                <CTableRow>
                                    {Object.keys(results[0]).map((key) => (
                                        <CTableHeaderCell key={key}>{key.replace(/^guest/, 'Guest ')}</CTableHeaderCell>
                                    ))}
                                </CTableRow>
                            </CTableHead>
                            <CTableBody>
                                {results.map((row, index) => (
                                    <CTableRow key={index}>
                                        {Object.values(row).map((value, idx) => (
                                            <CTableDataCell key={idx}>{value}</CTableDataCell>
                                        ))}
                                    </CTableRow>
                                ))}
                            </CTableBody>
                        </CTable>
                    ) : (
                        <div className={`p-2 rounded bg-light text-muted`}>No results to display.</div>
                    )}
                </div>
            </CCardBody>
        </CCard>
    );
};

export default ChatPanel;
