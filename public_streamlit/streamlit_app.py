import streamlit as st


# Write directly to the app
st.title("Stackoverflow Weekly Programming Newsletter :computer:")
st.write(
    """This newsletter was generated based on stackoverflow questions and answers using an AI model
    """
)

conn = st.connection("snowflake")
df = conn.query("select * from newsletters")
res_text = df.loc[df['CREATION_DATE'].idxmax()]['NEWSLETTER_BODY']
st.markdown(res_text)
