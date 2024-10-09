import React from 'react';
import { CToast, CToastBody, CToastHeader } from '@coreui/react';
import './ToastNotification.css';

const ToastNotification = ({ message, onClose, type }) => {
    return (
        <CToast autohide={true} visible={true} onClose={onClose}>
            <CToastHeader className={type === 'success' ? 'toast-success' : 'toast-error'} closeButton>
                <div className="fw-bold me-auto">{type === 'success' ? 'Success' : 'Error'}</div>
            </CToastHeader>
            <CToastBody>
                {message}
            </CToastBody>
        </CToast>
    );
};

export default ToastNotification;
