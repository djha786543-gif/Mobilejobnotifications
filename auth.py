import os
import streamlit as st


def require_password(page_key: str, env_var: str, label: str):
    """Password gate for a page. Call at the top of each Streamlit page.
    Reads the correct password from an environment variable.
    Stores auth state in session_state so the user only logs in once per session.
    """
    sess_key = f"auth_{page_key}"
    if st.session_state.get(sess_key):
        return

    st.markdown(f"## 🔒 {label}")
    pwd = st.text_input("Password", type="password", key=f"pwd_input_{page_key}")
    if st.button("Login", key=f"pwd_btn_{page_key}", type="primary"):
        correct = os.getenv(env_var, "")
        if not correct:
            st.error("Password not configured on server. Set the environment variable.")
        elif pwd == correct:
            st.session_state[sess_key] = True
            st.rerun()
        else:
            st.error("Incorrect password.")
    st.stop()
