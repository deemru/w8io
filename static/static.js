$(document).ready( function()
{
    console.log( document.cookie )
    var z = new Date().getTimezoneOffset();
    if( z % 1 === 0 )
    {
        var cookie = "z=" + ( -z ) + "m";
        if( document.cookie.indexOf( cookie ) === -1 )
        {
            document.cookie = cookie + "; max-age=31536000; path=/";
            document.location.replace( document.location.href );
        }
    }

    g_loading = false;
    g_lazyload = $(".lazyload");
    if( g_lazyload.length )
    {
        $(window).scroll( lazyload );
        lazyload();
    }

    $("a#L").click( function( e )
    {
        e.preventDefault();
        if( document.cookie.indexOf( "L=1" ) === -1 )
            document.cookie = "L=1; max-age=31536000; path=/";
        else
            document.cookie = "L=0; max-age=31536000; path=/";
        document.location.replace( document.location.href );
        return false;
    } );
} );

function lazyload()
{
    if( g_loading )
        return;

    var wt = typeof this.scrollY !== "undefined" ? this.scrollY : $(window).scrollTop();
    var wb = wt + this.innerHeight;
    var ot = g_lazyload.offset().top;
    var ob = ot + g_lazyload.height();

    if( wt <= ob && wb >= ot )
    {
        g_loading = true;
        $.get( g_lazyload.attr( "url" ), null, function( data )
        {
            if( data )
            {
                $( data ).insertAfter( g_lazyload );
                g_lazyload.remove();

                g_lazyload = $(".lazyload");
                if( g_lazyload.length )
                {
                    g_loading = false;
                    return lazyload();
                }
            }

            $(window).off( "scroll", lazyload );
        } );
    }
}