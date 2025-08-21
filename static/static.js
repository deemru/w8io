var g_loading = false;
var g_lazyload = null;
var g_lazytick = null;

$(document).ready( function()
{
    var z = new Date().getTimezoneOffset();
    var cookie = "z=" + ( -z ) + "m";
    if( document.cookie.indexOf( cookie ) === -1 )
    {
        document.cookie = cookie + "; max-age=31536000; path=/";
        document.location.replace( document.location.href );
    }

    g_loading = false;
    g_lazyload = $(".lazyload");
    if( g_lazyload.length )
    {
        $(window).scroll( lazytick );
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

function lazytick()
{
    if( g_lazytick )
        clearTimeout( g_lazytick );
    g_lazytick = setTimeout( function(){ lazyload(); }, 100 );
}

function lazyload()
{
    if( g_loading || !g_lazyload || !g_lazyload.length )
        return;

    var wt = ( typeof window.scrollY !== "undefined" ) ? window.scrollY : $(window).scrollTop();
    var wb = wt + window.innerHeight;

    if( !g_lazyload.offset )
    {
        g_lazyload = $(".lazyload");
        if( !g_lazyload.length )
        {
            $(window).off( "scroll", lazytick );
            return;
        }
    }

    var ot = g_lazyload.offset().top;
    var ob = ot + g_lazyload.height();

    if( wt <= ob && wb >= ot )
    {
        g_loading = true;

        var url = g_lazyload.attr( "url" );
        if( !url )
        {
            console.error( "lazyload missing 'url'" );
            g_loading = false;
            $(window).off( "scroll", lazytick );
            return;
        }

        $.get( url )
            .done( function( data )
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

                g_loading = false;
                $(window).off( "scroll", lazytick );
            } )
            .fail( function( xhr, status, error )
            {
                console.error( "lazyload request failed: " + status + " (" + error + ") - retrying in 8s" );
                setTimeout( function()
                {
                    g_loading = false;
                    lazyload();
                }, 8000 );
            } );
    }
}