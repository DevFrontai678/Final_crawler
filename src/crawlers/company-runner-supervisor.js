'use strict';

const { fork: defaultFork } = require('child_process');
const fs = require('fs');
const path = require('path');
const activeChildren = new Set();

function linuxCgroupPath() {
    if (process.platform !== 'linux') return null;
    const root = process.env.CRAWLER_CGROUP_ROOT || '/sys/fs/cgroup';
    if (!fs.existsSync(path.join(root, 'cgroup.controllers'))) return null;
    const name = `customer-matching-crawler-${process.pid}-${Date.now()}-${Math.random().toString(16).slice(2)}`;
    const target = path.join(root, name);
    try {
        fs.mkdirSync(target);
        if (!fs.existsSync(path.join(target, 'cgroup.kill'))) {
            fs.rmSync(target, { recursive: true, force: true });
            return null;
        }
        return target;
    } catch {
        return null;
    }
}

function addPidToCgroup(cgroupPath, pid) {
    if (!cgroupPath) return false;
    try {
        fs.writeFileSync(path.join(cgroupPath, 'cgroup.procs'), String(pid));
        return true;
    } catch {
        return false;
    }
}

function cleanupCgroup(cgroupPath) {
    if (!cgroupPath) return;
    try { fs.rmSync(cgroupPath, { recursive: true, force: true }); } catch {}
}

function linuxDescendants(rootPid) {
    if (process.platform !== 'linux') return [];
    const children = new Map();
    try {
        for (const entry of fs.readdirSync('/proc')) {
            if (!/^\d+$/.test(entry)) continue;
            try {
                const stat = fs.readFileSync(`/proc/${entry}/stat`, 'utf8');
                const afterComm = stat.slice(stat.lastIndexOf(')') + 2).split(' ');
                const ppid = Number(afterComm[1]);
                if (!Number.isInteger(ppid)) continue;
                if (!children.has(ppid)) children.set(ppid, []);
                children.get(ppid).push(Number(entry));
            } catch {}
        }
    } catch {}
    const result = [];
    const visit = pid => {
        for (const child of children.get(pid) || []) {
            result.push(child);
            visit(child);
        }
    };
    visit(rootPid);
    return result.reverse();
}

function wait(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function sendSignal(child, signal, cgroupPath = null) {
    if (!child || !child.pid) return false;
    try {
        // Detached children have their own process group on Linux/macOS. Killing
        // the group also terminates Chromium descendants spawned by Playwright.
        if (process.platform === 'linux' || process.platform === 'darwin') {
            process.kill(-child.pid, signal);
        } else if (typeof child.kill === 'function') {
            child.kill(signal);
        }
        return true;
    } catch {
        try {
            return Boolean(child.kill?.(signal));
        } catch {
            return false;
        }
    }
}

function terminateProcessTree(child, signal, cgroupPath = null) {
    if (process.platform === 'linux') {
        for (const pid of linuxDescendants(child.pid)) {
            try { process.kill(pid, signal); } catch {}
        }
        sendSignal(child, signal, cgroupPath);
        if (signal === 'SIGKILL' && cgroupPath) {
            try { fs.writeFileSync(path.join(cgroupPath, 'cgroup.kill'), '1'); } catch {}
        }
        return;
    }
    sendSignal(child, signal, cgroupPath);
}

function waitForExit(child, timeoutMs) {
    if (child.exitCode !== null || child.signalCode) return Promise.resolve(true);
    return new Promise(resolve => {
        let timer = setTimeout(() => {
            timer = null;
            resolve(false);
        }, timeoutMs);
        child.once('exit', () => {
            if (timer) clearTimeout(timer);
            timer = null;
            resolve(true);
        });
    });
}

/**
 * Run exactly one company in an isolated process. The parent never invokes
 * processCompany itself and never waits on a child promise without a deadline.
 */
function runIsolatedCompany({
    jobData,
    runnerPath,
    timeoutMs,
    terminationGraceMs,
    forkImpl = defaultFork,
    runnerArgs = ['--company-runner'],
    env = process.env,
    onTimeout
}) {
    const cgroupPath = linuxCgroupPath();
    let child;
    try {
        child = forkImpl(runnerPath, runnerArgs, {
            detached: process.platform === 'linux' || process.platform === 'darwin',
            stdio: ['ignore', 'inherit', 'inherit', 'ipc'],
            env: {
                ...env,
                CRAWLER_COMPANY_RUNNER: '1',
                ...(cgroupPath ? { CRAWLER_RUNNER_CGROUP_PATH: cgroupPath } : {})
            }
        });
    } catch (error) {
        return Promise.resolve({
            status: 'failed',
            companyId: jobData?.companyId,
            companyName: jobData?.companyName,
            jobsSaved: 0,
            error: error.message
        });
    }
    if (cgroupPath) addPidToCgroup(cgroupPath, child.pid);
    const metadata = { child, cgroupPath };
    activeChildren.add(metadata);

    return new Promise(resolve => {
        let settled = false;
        let timedOut = false;
        let normalResultPending = false;
        let terminating = false;
        let pendingResult = null;
        let deadlineTimer = null;
        let forceTimer = null;

        const resultOnTimeout = {
            status: 'partial',
            companyId: jobData?.companyId,
            companyName: jobData?.companyName,
            jobsSaved: 0,
            elapsedMs: timeoutMs,
            metrics: { timed_out: true, partialReason: 'company_timeout' }
        };

        const resolveOnce = result => {
            if (settled) return;
            settled = true;
            if (deadlineTimer) clearTimeout(deadlineTimer);
            if (forceTimer) clearTimeout(forceTimer);
            activeChildren.delete(metadata);
            cleanupCgroup(cgroupPath);
            resolve(result);
        };

        const forceTerminate = async resultAfterTermination => {
            terminateProcessTree(child, 'SIGKILL', cgroupPath);
            await waitForExit(child, terminationGraceMs);
            resolveOnce(resultAfterTermination || resultOnTimeout);
        };

        const terminateAfterDeadline = async () => {
            if (settled || timedOut || normalResultPending || terminating) return;
            timedOut = true;
            try { onTimeout?.(); } catch {}
            terminating = true;
            pendingResult = resultOnTimeout;
            terminateProcessTree(child, 'SIGTERM', cgroupPath);
            forceTimer = setTimeout(() => { void forceTerminate(pendingResult); }, terminationGraceMs);
        };

        const terminateAfterError = error => {
            if (settled || terminating || normalResultPending) return;
            terminating = true;
            pendingResult = {
                status: 'failed',
                companyId: jobData?.companyId,
                companyName: jobData?.companyName,
                jobsSaved: 0,
                error: error?.message || 'Company runner IPC failure'
            };
            terminateProcessTree(child, 'SIGTERM', cgroupPath);
            forceTimer = setTimeout(() => { void forceTerminate(pendingResult); }, terminationGraceMs);
        };

        child.on('message', message => {
            if (settled || timedOut || !message) return;
            if (message.type === 'completed') {
                normalResultPending = true;
                if (deadlineTimer) {
                    clearTimeout(deadlineTimer);
                    deadlineTimer = null;
                }
                // Do not leave a successfully finished runner behind. Give it a
                // bounded opportunity to exit, then use the same hard kill path.
                (async () => {
                    const exited = await waitForExit(child, terminationGraceMs);
                    if (!exited) await forceTerminate(message.result);
                    else resolveOnce(message.result);
                })();
            }
        });

        child.on('error', error => {
            terminateAfterError(error);
        });

        child.on('exit', (code, signal) => {
            activeChildren.delete(metadata);
            cleanupCgroup(cgroupPath);
            if (settled || normalResultPending) return;
            if (terminating) {
                resolveOnce(pendingResult || resultOnTimeout);
                return;
            }
            resolveOnce({
                status: code === 0 ? 'completed' : 'failed',
                companyId: jobData?.companyId,
                companyName: jobData?.companyName,
                jobsSaved: 0,
                error: signal ? `company runner terminated by ${signal}` : `company runner exited with code ${code}`
            });
        });

        deadlineTimer = setTimeout(() => { void terminateAfterDeadline(); }, timeoutMs);
        try {
            child.send({ type: 'run', jobData }, error => {
                if (error) terminateAfterError(error);
            });
        } catch (error) {
            terminateAfterError(error);
        }
    });
}

module.exports = {
    runIsolatedCompany,
    sendSignal,
    waitForExit,
    forceTerminateAllCompanyRunners() {
        for (const metadata of activeChildren) {
            terminateProcessTree(metadata.child, 'SIGKILL', metadata.cgroupPath);
        }
    }
};
