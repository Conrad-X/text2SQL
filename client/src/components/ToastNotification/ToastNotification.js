import React from 'react';
import { CToast, CToastBody, CToastHeader } from '@coreui/react';
import { TOAST_TYPE } from 'constants/toastType';
import './ToastNotification.css';

const ToastNotification = ({ message, onClose, type }) => {
    return (
        <CToast autohide={true} visible={true} onClose={onClose}>
            <CToastHeader className={type === TOAST_TYPE.SUCCESS ? 'toast-success' : 'toast-error'} closeButton>
                <div className="fw-bold me-auto">{type === TOAST_TYPE.SUCCESS ? 'Success' : 'Error'}</div>
            </CToastHeader>
            <CToastBody>
                {message}
            </CToastBody>
        </CToast>
    );
};

export default ToastNotification;
