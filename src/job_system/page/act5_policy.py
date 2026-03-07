import streamlit as st
import streamlit.components.v1 as components

def act5_render():
    st.title("車禍數據看板")

<<<<<<< HEAD
=======
    if st.session_state.page != "政策來檢驗":
        return

>>>>>>> Tom
    st.markdown("""
<hr style="
    border: 0;
    height: 6px;
    background: linear-gradient(90deg, #e53935, #ff8a80);
    border-radius: 3px;
">
""", unsafe_allow_html=True)

    # 建立三個分頁來存放不同的看板
    tab1, tab2, tab3 = st.tabs(["政策有效嗎 ？", "車禍趨勢", "車禍肇因"])

    with tab1:
        st.subheader("政策成效")
        html_code_2 = """
        <div class='tableauPlaceholder' id='viz1772686598899' style='position: relative'><noscript><a href='#'><img alt=' ' src='https://public.tableau.com/static/images/BZ/BZD7KXXBD/1_rss.png' style='border: none' /></a></noscript><object class='tableauViz'  style='display:none;'><param name='host_url' value='https%3A%2F%2Fpublic.tableau.com%2F' /> <param name='embed_code_version' value='3' /> <param name='path' value='shared/BZD7KXXBD' /> <param name='toolbar' value='yes' /><param name='static_image' value='https://public.tableau.com/static/images/BZ/BZD7KXXBD/1.png' /> <param name='animate_transition' value='yes' /><param name='display_static_image' value='yes' /><param name='display_spinner' value='yes' /><param name='display_overlay' value='yes' /><param name='display_count' value='yes' /><param name='language' value='zh-TW' /></object></div>                <script type='text/javascript'>                
        var divElement = document.getElementById('viz1772686598899');                   
        var vizElement = divElement.getElementsByTagName('object')[0];                 
        vizElement.style.width='1000px';vizElement.style.height='850px';                  
        var scriptElement = document.createElement('script');                   
        scriptElement.src = 'https://public.tableau.com/javascripts/api/viz_v1.js';                    vizElement.parentNode.insertBefore(scriptElement, vizElement);              
        </script>
        """
        components.html(html_code_2, height=850, scrolling=True)

    with tab2:
        st.subheader("案件分析")
        html_code_1 = """
        <div class='tableauPlaceholder' id='viz1772686396223' style='position: relative'><noscript><a href='#'><img alt=' ' src='https://public.tableau.com/static/images/tj/tjr104_mart/1/1_rss.png' style='border: none' /></a></noscript><object class='tableauViz'  style='display:none;'><param name='host_url' value='https%3A%2F%2Fpublic.tableau.com%2F' /> <param name='embed_code_version' value='3' /> <param name='site_root' value='' /><param name='name' value='tjr104_mart/1' /><param name='tabs' value='no' /><param name='toolbar' value='yes' /><param name='static_image' value='https://public.tableau.com/static/images/tj/tjr104_mart/1/1.png' /> <param name='animate_transition' value='yes' /><param name='display_static_image' value='yes' /><param name='display_spinner' value='yes' /><param name='display_overlay' value='yes' /><param name='display_count' value='yes' /><param name='language' value='zh-TW' /></object></div>                <script type='text/javascript'>                    var divElement = document.getElementById('viz1772686396223');                   
        var vizElement = divElement.getElementsByTagName('object')[0];                  
        if ( divElement.offsetWidth > 800 ) { vizElement.style.minWidth='1000px';vizElement.style.maxWidth='100%';vizElement.style.minHeight='850px';vizElement.style.maxHeight=(divElement.offsetWidth*0.75)+'px';} else if ( divElement.offsetWidth > 500 ) { vizElement.style.minWidth='1000px';vizElement.style.maxWidth='100%';vizElement.style.minHeight='850px';vizElement.style.maxHeight=(divElement.offsetWidth*0.75)+'px';} else { vizElement.style.minWidth='1000px';vizElement.style.maxWidth='100%';vizElement.style.minHeight='850px';vizElement.style.maxHeight=(divElement.offsetWidth*1.77)+'px';}                   
        var scriptElement = document.createElement('script');                  
        scriptElement.src = 'https://public.tableau.com/javascripts/api/viz_v1.js';                    vizElement.parentNode.insertBefore(scriptElement, vizElement);                </script>
        """
        components.html(html_code_1, height=850, scrolling=True)

    with tab3:
        st.subheader("車禍分析")
        html_code_3 = """
        <div class='tableauPlaceholder' id='viz1772686633136' style='position: relative'><noscript><a href='#'><img alt=' ' src='https://public.tableau.com/static/images/ZX/ZXBD29HNG/1_rss.png' style='border: none' /></a></noscript><object class='tableauViz'  style='display:none;'><param name='host_url' value='https%3A%2F%2Fpublic.tableau.com%2F' /> <param name='embed_code_version' value='3' /> <param name='path' value='shared/ZXBD29HNG' /> <param name='toolbar' value='yes' /><param name='static_image' value='https://public.tableau.com/static/images/ZX/ZXBD29HNG/1.png' /> <param name='animate_transition' value='yes' /><param name='display_static_image' value='yes' /><param name='display_spinner' value='yes' /><param name='display_overlay' value='yes' /><param name='display_count' value='yes' /><param name='language' value='zh-TW' /></object></div>                <script type='text/javascript'>                   
        var divElement = document.getElementById('viz1772686633136');               
        var vizElement = divElement.getElementsByTagName('object')[0];                
        vizElement.style.width='1000px';vizElement.style.height='850px';               
        var scriptElement = document.createElement('script');                  
        scriptElement.src = 'https://public.tableau.com/javascripts/api/viz_v1.js';                    vizElement.parentNode.insertBefore(scriptElement, vizElement);              
        </script>
        """
        components.html(html_code_3, height=850, scrolling=True)