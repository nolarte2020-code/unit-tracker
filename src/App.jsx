import { useEffect, useState } from "react";
import { supabase } from "./supabase";
import Auth from "./Auth";
import AppDashboard from "./AppDashboard";

export default function App() {
  const [session, setSession] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    supabase.auth.getSession().then(({ data }) => {
      setSession(data?.session ?? null);
      setLoading(false);
    });

    const { data: sub } = supabase.auth.onAuthStateChange((_event, nextSession) => {
      setSession(nextSession);
    });

    return () => sub?.subscription?.unsubscribe();
  }, []);

  if (loading) return <div style={{ padding: 20, fontFamily: "system-ui" }}>Loading...</div>;

  if (!session) return <Auth onAuthed={setSession} />;

  return <AppDashboard />;
}
