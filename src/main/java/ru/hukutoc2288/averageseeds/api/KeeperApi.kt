package ru.hukutoc2288.averageseeds.api

import retrofit2.Call
import retrofit2.http.GET
import retrofit2.http.Path
import ru.hukutoc2288.averageseeds.entities.keeper.ForumSize
import ru.hukutoc2288.averageseeds.entities.keeper.ForumTopicsInfo
import ru.hukutoc2288.averageseeds.entities.keeper.ForumTree

interface KeeperApi {

    @GET("v1/static/cat_forum_tree")
    fun catForumTree(): Call<ForumTree>

    @GET("v1/static/forum_size")
    fun forumSize(): Call<ForumSize>


    @GET("v1/static/pvc/f/{forum_id}")
    fun getForumTorrents(@Path("forum_id") forumId: Int): Call<ForumTopicsInfo>
}