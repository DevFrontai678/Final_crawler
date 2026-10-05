'use strict';

const { spawn } = require('child_process');

process.on('message', message => {
    if (message?.type !== 'run') return;
    if (process.env.COMPANY_RUNNER_FIXTURE_MODE === 'normal') {
        process.send?.({
            type: 'completed',
            result: { status: 'completed', companyId: message.jobData.companyId, jobsSaved: 2 }
        }, () => process.exit(0));
        return;
    }

    if (process.env.COMPANY_RUNNER_FIXTURE_MODE === 'descendant') {
        spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { stdio: 'ignore' });
    }
    // Deliberately never reply or exit. The supervisor must force terminate it.
});
