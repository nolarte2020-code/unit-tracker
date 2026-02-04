import { useEffect, useState } from "react";
import { supabase } from "./supabase";

export default function Auth({ onAuthed }) {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState("");

  useEffect(() => {
    supabase.auth.getSession().then(({ data }) => {
      if (data?.session) onAuthed(data.session);
    });

    const { data: sub } = supabase.auth.onAuthStateChange((_event, session) => {
      if (session) onAuthed(session);
    });

    return () => sub?.subscription?.unsubscribe();
  }, [onAuthed]);

  async function signIn(e) {
    e.preventDefault();
    setBusy(true);
    setMsg("");

    const { error } = await supabase.auth.signInWithPassword({
      email,
      password,
    });

    if (error) setMsg(error.message);
    setBusy(false);
  }

  return (
    <div style={{ minHeight: "100vh", display: "grid", placeItems: "center", padding: 20, fontFamily: "system-ui" }}>
      <div style={{ width: "100%", maxWidth: 420, border: "1px solid rgba(0,0,0,0.12)", borderRadius: 12, padding: 18 }}>
        <h2 style={{ margin: 0 }}>Unit Tracker</h2>
        <p style={{ marginTop: 6, opacity: 0.75 }}>Please sign in to continue.</p>

        <form onSubmit={signIn}>
          <label style={{ display: "block", marginTop: 12, marginBottom: 6 }}>Email</label>
          <input
            style={{ width: "100%", padding: 10, borderRadius: 10, border: "1px solid rgba(0,0,0,0.18)" }}
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            autoComplete="email"
            placeholder="you@company.com"
          />

          <label style={{ display: "block", marginTop: 12, marginBottom: 6 }}>Password</label>
          <input
            style={{ width: "100%", padding: 10, borderRadius: 10, border: "1px solid rgba(0,0,0,0.18)" }}
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            autoComplete="current-password"
            placeholder="••••••••"
          />

          <button style={{ marginTop: 14, width: "100%", padding: 10, borderRadius: 10, border: "none", cursor: "pointer" }} disabled={busy} type="submit">
            {busy ? "Signing in..." : "Sign In"}
          </button>

          {msg ? <div style={{ marginTop: 10, color: "crimson" }}>{msg}</div> : null}
        </form>
      </div>
    </div>
  );
}
