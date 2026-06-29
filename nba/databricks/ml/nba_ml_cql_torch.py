# Databricks notebook source
"""Discrete CQL + Behavior Cloning in PURE PyTorch — no d3rlpy, so it installs nothing on Databricks serverless
(torch is in the base). This IS the algorithm d3rlpy runs, written out: a Q-network trained with the Bellman/TD
update PLUS the CQL *conservative* penalty (push Q DOWN on all actions, UP on the dataset action -> the offline
policy can't over-trust actions the data barely covers). Greedy-on-Q = the policy. %run-loadable on Databricks and
importable locally. Used by nba_ml_rl_train_local.py (proof) + nba_ml_rl_train.py (production)."""
import numpy as np, torch, torch.nn as nn


def _mlp(inp, out, hidden=(256, 256)):
    layers, d = [], inp
    for h in hidden:
        layers += [nn.Linear(d, h), nn.ReLU()]; d = h
    layers += [nn.Linear(d, out)]
    return nn.Sequential(*layers)


def train_cql(O, A, R, S2, DONE, state_dim, action_dim, device="cpu",
              steps=80000, batch=512, gamma=0.99, alpha=0.15, lr=1e-4, target_update=2000, tau=0.005, log=print):
    """Offline discrete CQL. O,A,R,S2,DONE = numpy transition arrays. Returns the trained Q-network (torch).
    Stabilized like a production DQN: Huber TD loss, gradient clipping, and POLYAK (soft) target updates."""
    Q = _mlp(state_dim, action_dim).to(device)
    Qt = _mlp(state_dim, action_dim).to(device); Qt.load_state_dict(Q.state_dict())
    opt = torch.optim.Adam(Q.parameters(), lr=lr)
    Ot = torch.as_tensor(O, dtype=torch.float32, device=device); At = torch.as_tensor(A, dtype=torch.long, device=device)
    Rt = torch.as_tensor(R, dtype=torch.float32, device=device); S2t = torch.as_tensor(S2, dtype=torch.float32, device=device)
    Dt = torch.as_tensor(DONE, dtype=torch.float32, device=device); n = len(O)
    huber = nn.SmoothL1Loss()
    for step in range(steps):
        idx = torch.randint(0, n, (batch,), device=device)
        s, a, r, s2, d = Ot[idx], At[idx], Rt[idx], S2t[idx], Dt[idx]
        q = Q(s); q_sa = q.gather(1, a[:, None]).squeeze(1)
        with torch.no_grad():
            target = r + gamma * (1.0 - d) * Qt(s2).max(1)[0]            # Bellman target (terminal-masked)
        td = huber(q_sa, target)                                         # robust TD / Bellman update
        conservative = (torch.logsumexp(q, dim=1) - q_sa).mean()         # CQL: down on all, up on the data action
        loss = td + alpha * conservative
        opt.zero_grad(); loss.backward()
        nn.utils.clip_grad_norm_(Q.parameters(), 10.0)
        opt.step()
        with torch.no_grad():                                           # polyak soft target update (stable)
            for p, pt in zip(Q.parameters(), Qt.parameters()): pt.mul_(1 - tau).add_(tau * p)
        if step % max(1, steps // 6) == 0:
            log(f"    cql step {step}: td={td.item():.3f} conservative={conservative.item():.3f}")
    return Q


def train_bc(O, A, state_dim, action_dim, device="cpu", steps=6000, batch=256, lr=1e-3):
    """Behavior cloning baseline: classify (state -> logged action). Returns the policy net."""
    P = _mlp(state_dim, action_dim).to(device); opt = torch.optim.Adam(P.parameters(), lr=lr)
    Ot = torch.as_tensor(O, dtype=torch.float32, device=device); At = torch.as_tensor(A, dtype=torch.long, device=device)
    ce = nn.CrossEntropyLoss(); n = len(O)
    for _ in range(steps):
        idx = torch.randint(0, n, (batch,), device=device)
        loss = ce(P(Ot[idx]), At[idx]); opt.zero_grad(); loss.backward(); opt.step()
    return P


def greedy(net, obs_batch, device="cpu"):
    with torch.no_grad():
        return net(torch.as_tensor(obs_batch, dtype=torch.float32, device=device)).argmax(1).cpu().numpy()


def qnet_layers(Q):
    """Extract the Q-net's Linear layers (forward order) as numpy weights for the serving scorer (matmuls only)."""
    lins = [m for m in Q.modules() if isinstance(m, nn.Linear)]
    return [{"W": l.weight.detach().cpu().numpy().tolist(), "b": l.bias.detach().cpu().numpy().tolist()} for l in lins]
