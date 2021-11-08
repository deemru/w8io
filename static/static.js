$(document).ready( function()
{
    g_loading = false;
    g_lazyload = $(".lazyload");
    if( g_lazyload.length )
    {
        $(window).scroll( lazyload );
        lazyload();
    }
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