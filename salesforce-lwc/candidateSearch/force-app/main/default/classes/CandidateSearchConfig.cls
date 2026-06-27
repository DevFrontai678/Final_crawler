public with sharing class CandidateSearchConfig {
    @AuraEnabled(cacheable=true)
    public static String getN8nConfig() {
        return 'PLACEHOLDER';
    }
}
