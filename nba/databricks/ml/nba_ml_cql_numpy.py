# Databricks notebook source
"""Discrete CQL + Behavior Cloning in PURE NUMPY — zero install on Databricks serverless (numpy is in EVERY base;
torch is not, and pip-installing it trips the immutable-package constraints). The Q-net is tiny (13->256->256->5),
so hand-rolled forward + backprop + Adam is exact and fast. This IS the algorithm: a Q-network trained with the
Bellman/TD update (Huber) PLUS the CQL *conservative* penalty (push Q DOWN on all actions via logsumexp, UP on the
dataset action -> the offline policy can't over-trust actions the data barely covers). Greedy-on-Q = the policy.
Weights are stored as torch-Linear-compatible (out,in) so the serving scorer's `X @ W.T + b` is unchanged. Validated
against the d3rlpy/torch reference (~76% Upgraded) by nba_ml_rl_train_local.py. %run-loadable + importable locally."""
import numpy as np


def _init(state_dim, action_dim, hidden=(256, 256), seed=0):
    rng = np.random.default_rng(seed); dims = [state_dim, *hidden, action_dim]; layers = []
    for i in range(len(dims) - 1):
        fan_in = dims[i]
        W = rng.standard_normal((dims[i + 1], dims[i])).astype(np.float32) * np.sqrt(2.0 / fan_in)
        layers.append((W, np.zeros(dims[i + 1], np.float32)))
    return layers


def _forward(layers, X):
    acts = [X]; pre = []
    for i, (W, b) in enumerate(layers):
        z = acts[-1] @ W.T + b; pre.append(z)
        acts.append(np.maximum(z, 0.0) if i < len(layers) - 1 else z)
    return acts[-1], (acts, pre)


def _backward(layers, cache, dout):
    acts, pre = cache; grads = [None] * len(layers); d = dout
    for i in reversed(range(len(layers))):
        W, _ = layers[i]
        grads[i] = (d.T @ acts[i], d.sum(0))
        if i > 0:
            d = (d @ W) * (pre[i - 1] > 0)
    return grads


class _Adam:
    def __init__(self, layers, lr=1e-4, b1=0.9, b2=0.999, eps=1e-8):
        self.lr, self.b1, self.b2, self.eps, self.t = lr, b1, b2, eps, 0
        self.m = [(np.zeros_like(W), np.zeros_like(b)) for W, b in layers]
        self.v = [(np.zeros_like(W), np.zeros_like(b)) for W, b in layers]

    def step(self, layers, grads):
        self.t += 1; out = []
        for i, ((W, b), (gW, gb)) in enumerate(zip(layers, grads)):
            new = []
            for p, g, m, v in ((W, gW, self.m[i][0], self.v[i][0]), (b, gb, self.m[i][1], self.v[i][1])):
                m[...] = self.b1 * m + (1 - self.b1) * g
                v[...] = self.b2 * v + (1 - self.b2) * (g * g)
                mhat = m / (1 - self.b1 ** self.t); vhat = v / (1 - self.b2 ** self.t)
                new.append(p - self.lr * mhat / (np.sqrt(vhat) + self.eps))
            out.append((new[0], new[1]))
        return out


def train_cql(O, A, R, S2, DONE, state_dim, action_dim, device=None,
              steps=80000, batch=512, gamma=0.99, alpha=0.15, lr=1e-4, tau=0.005, seed=0, log=print):
    """Offline discrete CQL (numpy). Returns the trained Q-net as a list of (W,b) layers (out,in)."""
    rng = np.random.default_rng(seed)
    O = O.astype(np.float32); A = A.astype(np.int64); R = R.astype(np.float32); S2 = S2.astype(np.float32); D = DONE.astype(np.float32)
    n = len(O); ar = np.arange(batch)
    Q = _init(state_dim, action_dim, seed=seed); Qt = [(W.copy(), b.copy()) for W, b in Q]
    opt = _Adam(Q, lr=lr)
    for step in range(steps):
        idx = rng.integers(0, n, batch)
        s, a, r, s2, d = O[idx], A[idx], R[idx], S2[idx], D[idx]
        q, cache = _forward(Q, s); q_sa = q[ar, a]
        qt2, _ = _forward(Qt, s2)
        target = r + gamma * (1.0 - d) * qt2.max(1)                       # Bellman target (terminal-masked, no grad)
        e = q_sa - target
        ge = np.clip(e, -1.0, 1.0) / batch                               # Huber TD gradient
        m = q.max(1, keepdims=True); ex = np.exp(q - m); sm = ex / ex.sum(1, keepdims=True)
        dq = (alpha / batch) * sm                                        # CQL conservative: + softmax (down on all)
        dq[ar, a] -= (alpha / batch)                                     # CQL conservative: up on the data action
        dq[ar, a] += ge                                                  # TD on the taken action
        grads = _backward(Q, cache, dq.astype(np.float32))
        # gradient clip (global L2 norm 10)
        gn = np.sqrt(sum(float((gW ** 2).sum() + (gb ** 2).sum()) for gW, gb in grads))
        if gn > 10.0:
            scl = 10.0 / (gn + 1e-8); grads = [(gW * scl, gb * scl) for gW, gb in grads]
        Q = opt.step(Q, grads)
        Qt = [((1 - tau) * Wt + tau * W, (1 - tau) * bt + tau * b) for (W, b), (Wt, bt) in zip(Q, Qt)]
        if step % max(1, steps // 6) == 0:
            td = np.where(np.abs(e) < 1, 0.5 * e * e, np.abs(e) - 0.5).mean()
            cons = ((m.squeeze(1) + np.log(ex.sum(1))) - q_sa).mean()    # logsumexp(q) - q_sa
            log(f"    cql step {step}: td={td:.3f} conservative={cons:.3f}")
    return Q


def train_bc(O, A, state_dim, action_dim, device=None, steps=6000, batch=256, lr=1e-3, seed=0):
    """Behavior cloning (numpy softmax classifier): (state -> logged action). Returns the policy net."""
    rng = np.random.default_rng(seed + 1)
    O = O.astype(np.float32); A = A.astype(np.int64); n = len(O); ar = np.arange(batch)
    P = _init(state_dim, action_dim, seed=seed + 1); opt = _Adam(P, lr=lr)
    for _ in range(steps):
        idx = rng.integers(0, n, batch); s, a = O[idx], A[idx]
        q, cache = _forward(P, s)
        m = q.max(1, keepdims=True); ex = np.exp(q - m); sm = ex / ex.sum(1, keepdims=True)
        dq = sm.copy(); dq[ar, a] -= 1.0; dq /= batch
        P = opt.step(P, _backward(P, cache, dq.astype(np.float32)))
    return P


def greedy(net, obs_batch, device=None):
    q, _ = _forward(net, np.asarray(obs_batch, np.float32))
    return q.argmax(1)


def qnet_layers(net):
    """The net IS already numpy (out,in) layers — serialize for the serving scorer (matmuls only)."""
    return [{"W": W.tolist(), "b": b.tolist()} for W, b in net]
