// wwwroot/chat.js
// Chat functionality for BIM Assistant

let currentUrn = null;

export function initChat() {
    const chatToggle = document.getElementById('chat-toggle');
    const chatModal = document.getElementById('chat-modal');
    const chatClose = document.getElementById('chat-close');
    const chatInput = document.getElementById('chat-input');
    const chatSend = document.getElementById('chat-send');
    const chatMessages = document.getElementById('chat-messages');

    // Toggle chat modal
    chatToggle.addEventListener('click', () => {
        chatModal.classList.toggle('open');
        if (chatModal.classList.contains('open')) {
            chatInput.focus();
        }
    });

    // Close chat modal
    chatClose.addEventListener('click', () => {
        chatModal.classList.remove('open');
    });

    // Send message on button click
    chatSend.addEventListener('click', () => {
        sendMessage();
    });

    // Send message on Enter key
    chatInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            sendMessage();
        }
    });

    async function sendMessage() {
        const question = chatInput.value.trim();
        if (!question) return;

        if (!currentUrn) {
            addBotMessage('Vui lòng chọn một mô hình BIM trước khi đặt câu hỏi.');
            return;
        }

        // Add user message
        addUserMessage(question);
        chatInput.value = '';

        // Disable input while processing
        chatInput.disabled = true;
        chatSend.disabled = true;

        // Show loading indicator
        const loadingMessage = addLoadingMessage();

        try {
            const response = await fetch('/api/chat', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    urn: currentUrn,
                    question: question,
                    debug: false
                })
            });

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const data = await response.json();

            // Remove loading message
            loadingMessage.remove();

            // Add bot response
            addBotMessage(data.answer);

        } catch (error) {
            console.error('Chat error:', error);
            loadingMessage.remove();
            addBotMessage('Xin lỗi, có lỗi xảy ra khi xử lý câu hỏi của bạn. Vui lòng thử lại.');
        } finally {
            chatInput.disabled = false;
            chatSend.disabled = false;
            chatInput.focus();
        }
    }

    function addUserMessage(text) {
        const messageDiv = document.createElement('div');
        messageDiv.className = 'chat-message user';
        messageDiv.innerHTML = `
            <div class="message-content">${escapeHtml(text)}</div>
        `;
        chatMessages.appendChild(messageDiv);
        scrollToBottom();
    }

    function addBotMessage(text) {
        const messageDiv = document.createElement('div');
        messageDiv.className = 'chat-message bot';
        messageDiv.innerHTML = `
            <div class="message-content">${escapeHtml(text)}</div>
        `;
        chatMessages.appendChild(messageDiv);
        scrollToBottom();
    }

    function addLoadingMessage() {
        const messageDiv = document.createElement('div');
        messageDiv.className = 'chat-message bot loading';
        messageDiv.innerHTML = `
            <div class="message-content">
                <div class="typing-indicator">
                    <span></span>
                    <span></span>
                    <span></span>
                </div>
            </div>
        `;
        chatMessages.appendChild(messageDiv);
        scrollToBottom();
        return messageDiv;
    }

    function scrollToBottom() {
        chatMessages.scrollTop = chatMessages.scrollHeight;
    }

    function escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
}

export function setCurrentUrn(urn) {
    currentUrn = urn;
}

export function getCurrentUrn() {
    return currentUrn;
}
