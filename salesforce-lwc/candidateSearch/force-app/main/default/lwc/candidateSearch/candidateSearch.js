import { LightningElement, track } from 'lwc';
import getN8nConfig from '@salesforce/apex/CandidateSearchConfig.getN8nConfig';

export default class CandidateSearch extends LightningElement {
    @track mcgId = '';
    @track resultFields = [];
    @track isLoading = false;
    @track errorMsg = '';
    @track hasResult = false;
    @track n8nWebhookUrl = '';

    connectedCallback() {
        getN8nConfig()
            .then(result => {
                this.n8nWebhookUrl = result;
            })
            .catch(error => {
                this.errorMsg = 'Config error: ' + error.message;
            });
    }

    handleInput(event) {
        this.mcgId = event.target.value;
        this.errorMsg = '';
        this.hasResult = false;
        this.resultFields = [];
    }

    async handleSearch() {
        if (!this.mcgId || this.mcgId.trim() === '') {
            this.errorMsg = 'Bitte MCG-ID eingeben!';
            return;
        }

        this.isLoading = true;
        this.errorMsg = '';
        this.hasResult = false;
        this.resultFields = [];

        try {
            const response = await fetch(this.n8nWebhookUrl, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ mcgId: this.mcgId.trim() })
            });

            if (!response.ok) {
                throw new Error('Server Fehler: ' + response.status);
            }

            const data = await response.json();

            if (!data || Object.keys(data).length === 0) {
                this.errorMsg = 'Kein Kandidat gefunden: ' + this.mcgId;
                return;
            }

            this.resultFields = Object.entries(data).map(([key, value]) => ({
                label: key,
                value: value !== null && value !== undefined ? String(value) : '-'
            }));

            this.hasResult = true;

        } catch (error) {
            this.errorMsg = 'Fehler: ' + error.message;
        } finally {
            this.isLoading = false;
        }
    }
}
