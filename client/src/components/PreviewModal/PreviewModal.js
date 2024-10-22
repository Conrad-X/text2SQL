import React from 'react';
import { CModal, CModalHeader, CModalTitle, CModalBody, CModalFooter, CButton } from '@coreui/react';

const PreviewModal = ({ title, content, show, setShow }) => {
    return (
        <CModal visible={show} onClose={() => setShow(false)} size="lg" scrollable={true}>
            <CModalHeader>
                <CModalTitle>{title}</CModalTitle>
            </CModalHeader>
            <CModalBody>
                <pre>{content}</pre>
            </CModalBody>
            <CModalFooter>
                <CButton color="primary" onClick={() => setShow(false)}>Close</CButton>
            </CModalFooter>
        </CModal>
    );
};

export default PreviewModal;
