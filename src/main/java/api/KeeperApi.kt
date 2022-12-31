import ru.hukutoc2288.jvmtlo.entities.keeper.*
import retrofit2.Call
import retrofit2.http.GET
import retrofit2.http.Path
import retrofit2.http.Query

interface KeeperApi {

    @GET("v1/static/cat_forum_tree")
    fun catForumTree(): Call<ForumTree>

    @GET("v1/static/forum_size")
    fun forumSize(): Call<ForumSize>

    @GET("v1/static/keepers_user_data")
    fun keepersUserData(): Call<ForumKeepers>

    @GET("v1/static/pvc/f/{forum_id}")
    fun getForumTorrents(@Path("forum_id") forumId: Int): Call<ForumTopicsInfo>

    @GET("v1/get_limit")
    fun getLimit(): Call<ForumLimit>

    @GET("v1/get_tor_topic_data")
    fun getTorrentTopicsData(@Query("val") topics: String,@Query("by") by: String = "topic_id"): Call<ForumTorrentTopicsData>
}