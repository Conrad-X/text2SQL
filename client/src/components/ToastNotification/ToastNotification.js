import React from 'react';
import { CToast, CToastBody, CToastHeader } from '@coreui/react';
import './ToastNotification.css'

const ToastNotification = ({ message, onClose }) => {
    return (
        <CToast autohide={true} visible={true} onClose={onClose}>
            <CToastHeader closeButton>
                <div className="fw-bold me-auto">Error</div>
            </CToastHeader>
            <CToastBody>{message}</CToastBody>
        </CToast>
    );
};

export default ToastNotification;
